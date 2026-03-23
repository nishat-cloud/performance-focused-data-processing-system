#include <cmath>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>

// Rolling window size for moving average calculation
constexpr int ROLLING_WINDOW = 3;

// Threshold (%) to flag price anomalies (sudden jumps)
constexpr double ANOMALY_THRESHOLD_PCT = 0.8;

// Holds aggregated state for each symbol during processing
struct SymbolState {
    long long total_volume = 0;        // Total traded volume
    double total_notional = 0.0;       // Sum of (price * volume) for VWAP
    std::deque<double> rolling_prices; // Sliding window for rolling mean
    double rolling_mean_last = 0.0;    // Latest rolling mean
    double last_price = -1.0;          // Previous price (for anomaly detection)
    double peak_price = -1.0;          // Highest observed price (for drawdown)
    double max_drawdown_pct = 0.0;     // Maximum drawdown percentage
    int anomaly_count = 0;             // Count of detected anomalies

    // Update state with a new tick (price + volume)
    void update(double price, int volume) {
        // Update volume and notional (used for VWAP calculation)
        total_volume += volume;
        total_notional += price * volume;

        // Maintain rolling window of recent prices
        rolling_prices.push_back(price);
        if (static_cast<int>(rolling_prices.size()) > ROLLING_WINDOW) {
            rolling_prices.pop_front();
        }

        // Compute rolling mean
        double rolling_sum = 0.0;
        for (double p : rolling_prices) {
            rolling_sum += p;
        }
        rolling_mean_last = rolling_sum / rolling_prices.size();

        // Detect price anomalies based on percentage jump
        if (last_price > 0.0) {
            double jump_pct = std::abs((price - last_price) / last_price) * 100.0;
            if (jump_pct >= ANOMALY_THRESHOLD_PCT) {
                anomaly_count++;
            }
        }

        // Track peak price for drawdown calculation
        if (price > peak_price) {
            peak_price = price;
        }

        // Compute max drawdown (percentage drop from peak)
        if (peak_price > 0.0) {
            double drawdown_pct = ((peak_price - price) / peak_price) * 100.0;
            if (drawdown_pct > max_drawdown_pct) {
                max_drawdown_pct = drawdown_pct;
            }
        }

        // Store current price for next iteration
        last_price = price;
    }
};

// Parses a CSV line into symbol, price, and volume
// Returns false if parsing fails or data is invalid
bool parse_line(const std::string& line, std::string& symbol, double& price, int& volume) {
    std::stringstream ss(line);
    std::string timestamp;
    std::string price_str;
    std::string volume_str;

    // Extract CSV fields
    if (!std::getline(ss, timestamp, ',')) return false;
    if (!std::getline(ss, symbol, ',')) return false;
    if (!std::getline(ss, price_str, ',')) return false;
    if (!std::getline(ss, volume_str, ',')) return false;

    // Convert string values to numeric types
    try {
        price = std::stod(price_str);
        volume = std::stoi(volume_str);
    } catch (...) {
        return false; // Handle malformed numeric data
    }

    return !symbol.empty();
}

int main(int argc, char* argv[]) {
    // Expect input CSV and output JSON file paths
    if (argc != 3) {
        std::cerr << "Usage: ./pipeline <input_csv> <output_json>\n";
        return 1;
    }

    // Open input file
    std::ifstream input(argv[1]);
    if (!input.is_open()) {
        std::cerr << "Failed to open input file\n";
        return 1;
    }

    // Map to store state per symbol
    std::unordered_map<std::string, SymbolState> states;
    std::string line;

    std::getline(input, line); // Skip header row

    // Process each line (tick data)
    while (std::getline(input, line)) {
        std::string symbol;
        double price = 0.0;
        int volume = 0;

        // Parse and validate line
        if (!parse_line(line, symbol, price, volume)) {
            continue;
        }

        // Update state for the given symbol
        states[symbol].update(price, volume);
    }

    // Open output file
    std::ofstream output(argv[2]);
    if (!output.is_open()) {
        std::cerr << "Failed to open output file\n";
        return 1;
    }

    // Write results in JSON format
    output << "{\n";
    bool first_symbol = true;

    for (const auto& entry : states) {
        const auto& symbol = entry.first;
        const auto& state = entry.second;

        // Compute VWAP (Volume Weighted Average Price)
        double vwap = state.total_volume > 0 ? state.total_notional / state.total_volume : 0.0;

        if (!first_symbol) {
            output << ",\n";
        }
        first_symbol = false;

        output << "  \"" << symbol << "\": {\n";
        output << "    \"total_volume\": " << state.total_volume << ",\n";
        output << std::fixed << std::setprecision(4);
        output << "    \"vwap\": " << vwap << ",\n";
        output << "    \"rolling_mean_last\": " << state.rolling_mean_last << ",\n";
        output << "    \"anomaly_count\": " << state.anomaly_count << ",\n";
        output << "    \"max_drawdown_pct\": " << state.max_drawdown_pct << "\n";
        output << "  }";
    }

    output << "\n}\n";

    // Final summary output
    std::cout << "Processed " << states.size() 
              << " symbols. Output written to " << argv[2] << "\n";

    return 0;
}
