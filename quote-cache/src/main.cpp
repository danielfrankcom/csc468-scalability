#include <ctime>
#include <iostream>
#include <string>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/asio.hpp>

using boost::asio::ip::tcp;

static const int INCOMING_PORT = 6000;

static const std::string OUTGOING_HOST = "localhost";
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

    void start() {
        // Get incoming quote request.
        std::string request;
        read_request(request);

        // Forward to quote server.
        boost::asio::io_service svc;
        client client(svc, OUTGOING_HOST, std::to_string(OUTGOING_PORT));
        client.send(request);

        // Read quote server response.
        std::string response;
        client.receive(response);

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
            new_connection->start();
            start_accept();
        }
    }

    tcp::acceptor acceptor_;
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

