// Internal libraries
#include <iostream>
#include <string>
#include <sstream>
#include <vector> // for std::vector
#include <cstdlib> // for std::get_env
// Threading utilities
#include <atomic>
#include <csignal>
#include <thread>
// Time parsing utilities
#include <chrono>
#include <ctime>
#include <iomanip>
// External libraries
#include <curl/curl.h> // libcURL for HTTP requests
#include <nlohmann/json.hpp> // nlohmann/json for parsing JSON
#include <pqxx/pqxx> // pqxx for Postgresql interaction
// Thread signal, set to true upon start.
std::atomic<bool> should_run{true};
// libcurl write callback
size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
	((std::string*)userp)->append((char*)contents, size * nmemb);
	return size * nmemb;
}
// Fetch RedisQ payload (zkillredisq.stream/listen.php)
std::string fetch_kill_mail(const std::string& queueID, int ttw_seconds) {
	CURL *curl = curl_easy_init();
	std::string readBuffer;
	// Check curl pointer
	if (!curl) {
		return readBuffer;
	}
	// Set up curl
	std::string url = "https://zkillredisq.stream/listen.php?queueID=" + queueID + "&ttw=" + std::to_string(ttw_seconds);
	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
	curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
	// Perform curl
	curl_easy_perform(curl);
	curl_easy_cleanup(curl);
	return readBuffer;
}
// Fetch an arbitrary URL and return pair(body, http_code), reusable for other than ESI
std::pair<std::string,long> fetch_url_with_code(const std::string& url, long timeout_seconds = 10) {
	CURL *curl = curl_easy_init();
	std::string readBuffer;
	long http_code = 0;
	// Check curl pointer
	if (!curl) {
		return {readBuffer, http_code};
	}
	// Set up curl
	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
	curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout_seconds);
	curl_easy_setopt(curl, CURLOPT_USERAGENT, "astrocartics-killupdater/1.0");
	// Perform curl
	CURLcode res = curl_easy_perform(curl);
	// Check output
	if (res == CURLE_OK) {
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
	} else {
		std::cerr << "curl error for " << url << ": " << curl_easy_strerror(res) << std::endl;
	}
	curl_easy_cleanup(curl);
	return {readBuffer, http_code};
}
// Parse ISO8601 UTC (YYYY-MM-DDTHH:MM:SSZ) to time_t UTC
bool parse_iso_utc_to_time_t(const std::string& iso, std::time_t &out) {
	std::tm tm = {};
	std::istringstream ss(iso);
	// Parse string stream
	ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
	// Check output
	if (ss.fail()) {
		return false;
	}
	out = timegm(&tm);
	return true;
}
// Signal handler for graceful termination
void sigint_handler(int) {
	should_run = false;
	std::cout << "SIGINT received: shutting down workers!" << std::endl;
}
// Threaded function
void worker_thread(int worker_id, std::string queueID, int ttw, std::string db_info) {
	pqxx::connection conn(db_info);
	// Check database connection
	if (!conn.is_open()) {
		std::cerr << "Worker: " << worker_id << " can't open database\n";
		return;
	}
	// Keep looping until false flag
	while (should_run) {
		std::cout << "Worker: " << worker_id << " fetching RedisQ payload!" << std::endl;
		std::string redis_body = fetch_kill_mail(queueID, ttw);
        	if (!should_run) {
			break;
		}
		// Check if redisQ sent anything back
		if (redis_body.empty()) {
			// Avoid tight loop on empty responses
			std::this_thread::sleep_for(std::chrono::milliseconds(200));
			continue;
		}
		// Try parsing body
		try {
			nlohmann::json data = nlohmann::json::parse(redis_body);
			// locate zkb
			nlohmann::json zkb;
			long long killmail_id = 0;
			std::string kill_hash;
			// Check for zkillboard compact
			if (data.contains("package") && data["package"].is_object()) {
				auto &pkg = data["package"];
				// Check for zkb
				if (pkg.contains("zkb") && !pkg["zkb"].is_null()) {
					zkb = pkg["zkb"];
				}
				// Check for kill id
				if (pkg.contains("killID") && !pkg["killID"].is_null()) {
					killmail_id = pkg["killID"].get<long long>(); // note capital D from json
				}
			} else {
     				// Fallback if other type of packaging appears
				// -------------------------------------------
				// Check for zkb
				if (data.contains("zkb") && !data["zkb"].is_null()) {
					zkb = data["zkb"];
				}
				// Check for kill id
				if (data.contains("killmail_id") && !data["killmail_id"].is_null()) {
					killmail_id = data["killmail_id"].get<long long>();
				}
			}
			// Checo if zkb is null
			if (zkb.is_null()) {
				std::cerr << "Worker: " << worker_id << " DEBUG: no zkb block in payload; SKIPPIN" << std::endl;
				continue;
			} else {
				std::cout << "Worker: " << worker_id << " PAYLOAD: parsing zkb block;" << std::endl;
			}
			// Extract other values, including hash
			double destroyed_value = zkb.value("destroyedValue", 0.0);
			double dropped_value = zkb.value("droppedValue", 0.0);
			double fitted_value = zkb.value("fittedValue", 0.0);
			double total_value = zkb.value("totalValue", 0.0);
			if (kill_hash.empty()) {
				kill_hash = zkb.value("hash", "");
			}
			// Make sure we have zkill id and hash
			if (killmail_id == 0 || kill_hash.empty()) {
				std::cerr << "Worker: " << worker_id << " DEBUG: missing kill id/hash; killmail_id=" << killmail_id << " hash=" << kill_hash << std::endl;
				continue;
			} else {
				std::cout << "Worker: " << worker_id << " PAYLOAD: kill id/hash; killmail_id=" << killmail_id << " hash=" << kill_hash << std::endl;
			}
			// Fetch ESI authoritative killmail
			std::string esi_url = "https://esi.evetech.net/killmails/" + std::to_string(killmail_id) + "/" + kill_hash + "/";
			auto [esi_body, http_code] = fetch_url_with_code(esi_url, 10);
			// Make sure we have results
			if (http_code != 200 || esi_body.empty()) {
				std::cerr << "Worker: " << worker_id << " ESI fetch failed (" << http_code << ") for " << esi_url << " SKIPPING " << std::endl;
				std::this_thread::sleep_for(std::chrono::seconds(5));
				continue;
			}
			// Begin parsing body
			nlohmann::json esi_json;
			try {
				esi_json = nlohmann::json::parse(esi_body);
			} catch (const nlohmann::json::parse_error &e) {
				std::cerr << "Worker: " << worker_id << " failed to parse ESI JSON: " << e.what() << " SKIPPING " << std::endl;
				std::this_thread::sleep_for(std::chrono::seconds(5));
				continue;
			}
			// Extract authoritative values from ESI
			std::string esi_killmail_time = esi_json.value("killmail_time", "");
			long long solar_system_id = esi_json.value("solar_system_id", 0LL);
			long long victim_ship = 0;
			long long kill_ship = 0;
			// Get victim ship_type_id and map to victim_ship
			if (esi_json.contains("victim") && esi_json["victim"].contains("ship_type_id") && !esi_json["victim"]["ship_type_id"].is_null()) {
				victim_ship = esi_json["victim"]["ship_type_id"].get<long long>();
			}
			// Get attacker who delivered final_blow and map to kill_ship
			if (esi_json.contains("attackers") && esi_json["attackers"].is_array()) {
				for (const auto &att : esi_json["attackers"]) {
					if (att.value("final_blow", false)) {
						if (att.contains("ship_type_id") && !att["ship_type_id"].is_null()) {
							kill_ship = att["ship_type_id"].get<long long>();
						}
						break;
					}
				}
			}
			// Validate ESI-provided required fields
			// victim_ship and kill_ship may be 0 if not provided
			if (esi_killmail_time.empty() || solar_system_id == 0 || victim_ship == 0) {
				std::cerr << "Worker: " << worker_id << " ESI response missing required fields (time/system/victim_ship. SKIPPING " << std::endl;
				std::this_thread::sleep_for(std::chrono::seconds(5));
				continue;
			}
			// Parse time from ESI. If parse fails, skip time.
			std::time_t unix_time = 0;
			if (!parse_iso_utc_to_time_t(esi_killmail_time, unix_time)) {
				std::cerr << "Worker: " << worker_id << " failed to parse ESI killmail_time '" << esi_killmail_time << "'. SKIPPING" << std::endl;
				std::this_thread::sleep_for(std::chrono::seconds(5));
				continue;
			}
			// Insert using authoritative ESI values for time/system/ship and zkb for values
			try {
				pqxx::work txn(conn);
				// Note: table schema expected to have victim_ship and kill_ship columns.
				txn.exec_params("INSERT INTO killmails (killmail_id, killmail_hash, solar_system_id, killmail_time, destroyed_value, dropped_value, fitted_value, total_value, victim_ship, kill_ship) "
					"VALUES ($1, $2, $3, to_timestamp($4), $5, $6, $7, $8, $9, $10) "
					"ON CONFLICT (killmail_id) DO NOTHING",
					killmail_id,
					kill_hash,
					solar_system_id,
					static_cast<double>(unix_time),
					destroyed_value,
					dropped_value,
					fitted_value,
					total_value,
					victim_ship,
					kill_ship);
				txn.commit();
				std::cout << "Worker: " << worker_id << " inserted kill " << killmail_id << " sys=" << solar_system_id << " victim_ship=" << victim_ship << " kill_ship=" << kill_ship << " time=" << esi_killmail_time << std::endl;
			} catch (const std::exception &e) {
				std::cerr << "Worker: " << worker_id << " database Error: " << e.what() << std::endl;
			}
		} catch (const nlohmann::json::parse_error &e) {
			std::cerr << "Worker: " << worker_id << " redisQ JSON parse error: " << e.what() << std::endl;
		} catch (const std::exception &e) {
			std::cerr << "Worker: " << worker_id << " error handling payload: " << e.what() << std::endl;
		}
	}
}
// Working together
int main() {
	// Initial cURL
	curl_global_init(CURL_GLOBAL_ALL);
	// Configuration for threads
	const int num_threads = 3; // Adjust to CPU, it is I/O so not necessarily limited to 1 thread per CPU.
	const std::string queueID = ""; // permanent ID
	const int ttw = 1; // 1 second
	std::string db_connection = "dbname= user= password= host=";
	// Set up signal handling for termination workers
	std::signal(SIGINT, sigint_handler);
	// Set up threads
	std::vector<std::thread> threads;
	// Add threads through iteration
	for (int i = 0; i < num_threads; ++i) {
		threads.emplace_back(worker_thread, i, queueID, ttw, db_connection);
	}
	// Wait for threads to shut down on signal
	for (auto& t : threads) {
		t.join();
	}
	// clean up cURL
	curl_global_cleanup();
	return 0;
}
