
using System;
using System.Collections.Generic;
using System.Text;
using System.Net;


//This project implements an echo socket server.
//The socket server requires four command line parameters:  
//Usage: AsyncSocketServer.exe <#connections> <Receive Size In Bytes> <address family: ipv4 | ipv6> <Local Port Number>

//# Connections: The maximum number of connections the server will accept simultaneously.
//Receive Size in Bytes: The buffer size used by the server for each receive operation.  
//Address family: The address family of the socket the server will use to listen for incoming connections.  Supported values are ‘ipv4’ and ‘ipv6’.
//Local Port Number: The port the server will bind to.

//Example: AsyncSocketServer.exe 500 1024 ipv4 8000



namespace CentralServer
{
    class Program
    {
        static void Main(string[] args)
        {
            int numConnections;
            int receiveSize;
            IPEndPoint localEndPoint;
            int port;

            
            numConnections = 10;
            receiveSize = 1500;
            port = 4000;
            
            localEndPoint = new IPEndPoint(IPAddress.Any, port);      

            //Console.WriteLine("Press any key to start the server ...");
            //Console.ReadKey();

            // Start the central server listening for incoming connection requests
            using (Server server = new Server(numConnections, receiveSize))
            {
                server.Init();
                server.Start(localEndPoint);
            }
        }
    }
}