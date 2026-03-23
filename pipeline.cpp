#include <cmath>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>

constexpr int ROLLING_WINDOW = 3;
constexpr double ANOMALY_THRESHOLD_PCT = 0.8;

struct SymbolState {
    long long total_volume = 0;
    double total_notional = 0.0;
    std::deque<double> rolling_prices;
    double rolling_mean_last = 0.0;
    double last_price = -1.0;
    double peak_price = -1.0;
    double max_drawdown_pct = 0.0;
    int anomaly_count = 0;

    void update(double price, int volume) {
        total_volume += volume;
        total_notional += price * volume;

        rolling_prices.push_back(price);
        if (static_cast<int>(rolling_prices.size()) > ROLLING_WINDOW) {
            rolling_prices.pop_front();
        }

        double rolling_sum = 0.0;
        for (double p : rolling_prices) {
            rolling_sum += p;
        }
        rolling_mean_last = rolling_sum / rolling_prices.size();

        if (last_price > 0.0) {
            double jump_pct = std::abs((price - last_price) / last_price) * 100.0;
            if (jump_pct >= ANOMALY_THRESHOLD_PCT) {
                anomaly_count++;
            }
        }

        if (price > peak_price) {
            peak_price = price;
        }
        if (peak_price > 0.0) {
            double drawdown_pct = ((peak_price - price) / peak_price) * 100.0;
            if (drawdown_pct > max_drawdown_pct) {
                max_drawdown_pct = drawdown_pct;
            }
        }

        last_price = price;
    }
};

bool parse_line(const std::string& line, std::string& symbol, double& price, int& volume) {
    std::stringstream ss(line);
    std::string timestamp;
    std::string price_str;
    std::string volume_str;

    if (!std::getline(ss, timestamp, ',')) return false;
    if (!std::getline(ss, symbol, ',')) return false;
    if (!std::getline(ss, price_str, ',')) return false;
    if (!std::getline(ss, volume_str, ',')) return false;

    try {
        price = std::stod(price_str);
        volume = std::stoi(volume_str);
    } catch (...) {
        return false;
    }
    return !symbol.empty();
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: ./pipeline <input_csv> <output_json>\n";
        return 1;
    }

    std::ifstream input(argv[1]);
    if (!input.is_open()) {
        std::cerr << "Failed to open input file\n";
        return 1;
    }

    std::unordered_map<std::string, SymbolState> states;
    std::string line;

    std::getline(input, line); // header

    while (std::getline(input, line)) {
        std::string symbol;
        double price = 0.0;
        int volume = 0;
        if (!parse_line(line, symbol, price, volume)) {
            continue;
        }
        states[symbol].update(price, volume);
    }

    std::ofstream output(argv[2]);
    if (!output.is_open()) {
        std::cerr << "Failed to open output file\n";
        return 1;
    }

    output << "{\n";
    bool first_symbol = true;
    for (const auto& entry : states) {
        const auto& symbol = entry.first;
        const auto& state = entry.second;
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

    std::cout << "Processed " << states.size() << " symbols. Output written to " << argv[2] << "\n";
    return 0;
}
