#include <chrono>
#include <iostream>
#include <string>
#include <unordered_map>
#include <iomanip>
#include <sstream>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/asio.hpp>

using boost::asio::ip::tcp;
using namespace std::chrono;

static const int INCOMING_PORT = 6000;

static const std::string OUTGOING_HOST = "quoteserve.seng.uvic.ca";
static const int OUTGOING_PORT = 4444;

class client {
public:
    client(boost::asio::io_service &svc, std::string const &host, std::string const &port)
            : io_service_(svc), socket_(io_service_) {
        boost::asio::ip::tcp::resolver resolver(io_service_);
        boost::asio::ip::tcp::resolver::iterator endpoint = resolver.resolve(
                boost::asio::ip::tcp::resolver::query(host, port));
        boost::asio::connect(this->socket_, endpoint);
    };

    void send(std::string const &message) {
        socket_.send(boost::asio::buffer(message));
    }

    void receive(std::string &response) {
        std::array<char, 128> buffer;
        boost::system::error_code error;
        size_t len = socket_.read_some(boost::asio::buffer(buffer), error);
        if (error == boost::asio::error::eof) {
            std::cout << "An error occurred while reading from a socket." << std::endl;
            return;
        }
        std::stringstream message_stream;
        message_stream.write(buffer.data(), len);

        std::string raw = message_stream.str();
        std::string stripped = raw.substr(0, raw.length() - 1); // Strip new line
        response = stripped;
    }

private:
    boost::asio::io_service &io_service_;
    boost::asio::ip::tcp::socket socket_;
};

class tcp_connection : public boost::enable_shared_from_this<tcp_connection> {
public:

    typedef boost::shared_ptr <tcp_connection> pointer;

    static pointer create(boost::asio::io_service &io_service) {
        return pointer(new tcp_connection(io_service));
    }

    tcp::socket &socket() {
        return socket_;
    }

    void start(std::unordered_map<std::string, std::tuple<milliseconds, std::string, double, std::string>>& cache) {
        // Get incoming quote request.
        std::string request;
        read_request(request);

        // Get stock symbol from request
        int delimiterPos = request.find(",");
        std::string stockSymbol = request.substr(0, delimiterPos);

        // Look up the stock symbol for a quote.
        std::string response;
        bool validCache = false;
        if (cache.find(stockSymbol) != cache.end()) {

            std::tuple<milliseconds, std::string, double, std::string> cachedValue = cache[stockSymbol];

            // Get timestamps to compare for quote expiry.
            milliseconds expiry = std::get<0>(cachedValue);
            milliseconds current = duration_cast<milliseconds>(
                    system_clock::now().time_since_epoch()
            );

            // Use the current quote if not expired.
            if (expiry >= current) {

                std::stringstream stream;

                // Force 2 decimal places on quote.
                double quote = std::get<2>(cachedValue);
                stream << std::fixed << std::setprecision(2) << quote << ",";

                stream << stockSymbol << ",";

                // Pull username from request, since user who requested cached version may not be the same.
                int usernameLength = response.length() - delimiterPos;
                std::string username = request.substr(delimiterPos + 1, usernameLength);
                stream << username << ",";

                // Place the cached server time into the response.
                std::string timestamp = std::get<1>(cachedValue);
                stream << timestamp << ",";

                std::string cryptokey = std::get<3>(cachedValue);
                stream << cryptokey;

                response = stream.str();
                validCache = true;

                std::cout << "Serving quote '" << request << "' from cache." << std::endl;
            }
        }

        // Grab a new quote if none is cached, or an expired quote is cached.
        if (!validCache) {

            // Forward to quote server.
            boost::asio::io_service svc;
            client client(svc, OUTGOING_HOST, std::to_string(OUTGOING_PORT));
            client.send(request);

            // Read quote server response.
            client.receive(response);


            // Grab the quote info for the cache.
            int endOfQuote = response.find(",", 0);
            std::cout << response << std::endl;
            double quote = std::stod(response.substr(0, endOfQuote));
            std::cout << quote << std::endl;

            // Ignore these as we already have info
            int endOfStockSymbol = response.find(",", endOfQuote + 1);
            int endOfUsername = response.find(",", endOfStockSymbol + 1);

            int endOfTimestamp = response.find(",", endOfUsername + 1);
            int startOfTimestamp = endOfUsername + 1;
            std::string timestamp = response.substr(startOfTimestamp, endOfTimestamp - startOfTimestamp);

            int endOfCryptokey = response.length() - 1;
            std::string cryptokey = response.substr(endOfTimestamp + 1, endOfCryptokey - endOfTimestamp);


            // Set expiry time for cached value.
            milliseconds current = duration_cast<milliseconds>(
                    system_clock::now().time_since_epoch()
            );
            milliseconds expiry = current + minutes(1);

            // Store the quote in the cache for later use.
            std::tuple<milliseconds, std::string, double, std::string> value(expiry, timestamp, quote, cryptokey);
            cache[stockSymbol] = value;

            std::cout << "No cache found for '" << request << "', contacting server." << std::endl;
        }

        // Send quote server response to client.
        boost::asio::async_write(socket_, boost::asio::buffer(response),
                                 boost::bind(&tcp_connection::handle_write, shared_from_this()));
    }

private:
    tcp_connection(boost::asio::io_service &io_service)
            : socket_(io_service) {
    }

    void handle_write() {
    }

    void read_request(std::string &request) {
        std::array<char, 128> buffer;
        boost::system::error_code error;
        size_t len = socket_.read_some(boost::asio::buffer(buffer), error);
        if (error == boost::asio::error::eof) {
            std::cout << "An error occurred while reading from a socket." << std::endl;
            return;
        }
        std::stringstream message_stream;
        message_stream.write(buffer.data(), len);

        std::string raw = message_stream.str();
        std::string stripped = raw.substr(0, raw.length() - 1); // Strip new line
        request = stripped;
    }

    tcp::socket socket_;
};

class tcp_server {
public:

    tcp_server(boost::asio::io_service &io_service)
            : acceptor_(io_service, tcp::endpoint(tcp::v4(), INCOMING_PORT)) {
        start_accept();
    }

private:
    void start_accept() {
        tcp_connection::pointer new_connection =
                tcp_connection::create(acceptor_.get_io_service());

        acceptor_.async_accept(new_connection->socket(),
                               boost::bind(&tcp_server::handle_accept, this, new_connection,
                                           boost::asio::placeholders::error));
    }

    void handle_accept(tcp_connection::pointer new_connection,
                       const boost::system::error_code &error) {
        if (!error) {
            new_connection->start(cache_);
            start_accept();
        }
    }

    tcp::acceptor acceptor_;

    // <expiry time, server time, quote price, cryptokey>
    std::unordered_map<std::string, std::tuple<milliseconds, std::string, double, std::string>> cache_;
};

int main() {
    try {
        boost::asio::io_service io_service;
        tcp_server server(io_service);

        std::cout << "Serving." << std::endl;
        io_service.run();
    }
    catch (std::exception &e) {
        std::cerr << e.what() << std::endl;
    }

    return 0;
}

