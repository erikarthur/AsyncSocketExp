
using System;
using System.Collections.Generic;
using System.Text;
using System.Net.Sockets;
using System.Net;
using System.Threading;


namespace CentralServer
{
    /// <summary>
    /// Implements the connection logic for the socket server.  After accepting a connection, all data read
    /// from the client is sent back to the client.  The read and echo back to the client pattern is continued 
    /// until the client disconnects.
    /// </summary>
    class Server : IDisposable
    {
        private int m_numConnections;   // the maximum number of connections the sample is designed to handle simultaneously 
        private int m_receiveBufferSize;// buffer size to use for each socket I/O operation 
        BufferManager m_bufferManager;  // represents a large reusable set of buffers for all socket operations
        const int opsToPreAlloc = 2;    // read, write (don't alloc buffer space for accepts)
        Socket listenSocket;            // the socket used to listen for incoming connection requests
        // pool of reusable SocketAsyncEventArgs objects for write, read and accept socket operations
        SocketAsyncEventArgsPool m_readWritePool;
        int m_totalBytesRead;           // counter of the total # bytes received by the server
        int m_numConnectedSockets;      // the total number of clients connected to the server 
        Semaphore m_maxNumberAcceptedClients;
        public List<peerInstance> peerList;

        /// <summary>
        /// Create an uninitialized server instance.  To start the server listening for connection requests
        /// call the Init method followed by Start method 
        /// </summary>
        /// <param name="numConnections">the maximum number of connections the sample is designed to handle simultaneously</param>
        /// <param name="receiveBufferSize">buffer size to use for each socket I/O operation</param>
        public Server(int numConnections, int receiveBufferSize)
        {
            m_totalBytesRead = 0;
            m_numConnectedSockets = 0;
            m_numConnections = numConnections;
            m_receiveBufferSize = receiveBufferSize;
            // allocate buffers such that the maximum number of sockets can have one outstanding read and 
            //write posted to the socket simultaneously  
            m_bufferManager = new BufferManager(receiveBufferSize * numConnections * opsToPreAlloc,
                receiveBufferSize);

            m_readWritePool = new SocketAsyncEventArgsPool(numConnections);
            m_maxNumberAcceptedClients = new Semaphore(numConnections, numConnections);
            peerList = new List<peerInstance>();
        }

        public void Dispose()
        {
            Dispose();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Initializes the server by preallocating reusable buffers and context objects.  These objects do not 
        /// need to be preallocated or reused, by is done this way to illustrate how the API can easily be used
        /// to create reusable objects to increase server performance.
        /// </summary>
        public void Init()
        {
            // Allocates one large byte buffer which all I/O operations use a piece of.  This gaurds 
            // against memory fragmentation
             m_bufferManager.InitBuffer();

            // preallocate pool of SocketAsyncEventArgs objects
            SocketAsyncEventArgs readWriteEventArg;

            for (int i = 0; i < m_numConnections; i++)
            {
                //Pre-allocate a set of reusable SocketAsyncEventArgs
                readWriteEventArg = new SocketAsyncEventArgs();
                readWriteEventArg.Completed += new EventHandler<SocketAsyncEventArgs>(IO_Completed);
                readWriteEventArg.UserToken = new AsyncUserToken();

                // assign a byte buffer from the buffer pool to the SocketAsyncEventArg object
                m_bufferManager.SetBuffer(readWriteEventArg);

                // add SocketAsyncEventArg to the pool
                m_readWritePool.Push(readWriteEventArg);
            }

        }

        /// <summary>
        /// Starts the server such that it is listening for incoming connection requests.    
        /// </summary>
        /// <param name="localEndPoint">The endpoint which the server will listening for conenction requests on</param>
        public void Start(IPEndPoint localEndPoint)
        {
            // create the socket which listens for incoming connections
            listenSocket = new Socket(localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            listenSocket.Bind(localEndPoint);
            
            // start the server with a listen backlog of 100 connections
            listenSocket.Listen(100);

            // post accepts on the listening socket
            StartAccept(null);

            //Console.WriteLine("{0} connected sockets with one outstanding receive posted to each....press any key", m_outstandingReadCount);
            Console.WriteLine("Press any key to terminate the server process....");
            Console.ReadKey();
        }


        /// <summary>
        /// Begins an operation to accept a connection request from the client 
        /// </summary>
        /// <param name="acceptEventArg">The context object to use when issuing the accept operation on the 
        /// server's listening socket</param>
        public void StartAccept(SocketAsyncEventArgs acceptEventArg)
        {
            if (acceptEventArg == null)
            {
                acceptEventArg = new SocketAsyncEventArgs();
                acceptEventArg.Completed += new EventHandler<SocketAsyncEventArgs>(AcceptEventArg_Completed);
            }
            else
            {
                // socket must be cleared since the context object is being reused
                acceptEventArg.AcceptSocket = null;
            }

            m_maxNumberAcceptedClients.WaitOne();
            bool willRaiseEvent = listenSocket.AcceptAsync(acceptEventArg);
            if (!willRaiseEvent)
            {
                ProcessAccept(acceptEventArg);
            }
        }

        /// <summary>
        /// This method is the callback method associated with Socket.AcceptAsync operations and is invoked
        /// when an accept operation is complete
        /// </summary>
        void AcceptEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            ProcessAccept(e);
        }

        private void ProcessAccept(SocketAsyncEventArgs e)
        {
            Interlocked.Increment(ref m_numConnectedSockets);
            Console.WriteLine("Client connection accepted. There are {0} clients connected to the server",
                m_numConnectedSockets);

            // Get the socket for the accepted client connection and put it into the 
            //ReadEventArg object user token
            SocketAsyncEventArgs readEventArgs = m_readWritePool.Pop();
            ((AsyncUserToken)readEventArgs.UserToken).Socket = e.AcceptSocket;

            //add code to update the peer 2 peer table and send a peer back to client.

            // As soon as the client is connected, post a receive to the connection
            bool willRaiseEvent = e.AcceptSocket.ReceiveAsync(readEventArgs);
            if (!willRaiseEvent)
            {
                ProcessReceive(readEventArgs);
            }

            // Accept the next connection request
            StartAccept(e);
        }

        /// <summary>
        /// This method is called whenever a receive or send opreation is completed on a socket 
        /// </summary> 
        /// <param name="e">SocketAsyncEventArg associated with the completed receive operation</param>
        void IO_Completed(object sender, SocketAsyncEventArgs e)
        {
            // determine which type of operation just completed and call the associated handler
            switch (e.LastOperation)
            {
                case SocketAsyncOperation.Receive:
                    ProcessReceive(e);
                    break;
                case SocketAsyncOperation.Send:
                    ProcessSend(e);
                    break;
                default:
                    throw new ArgumentException("The last operation completed on the socket was not a receive or send");
            }

        }

        //member function for parsing command messages.
        private commandMessage parseCommandMessage(byte[] buf)
        {
            commandMessage returnMsg = new commandMessage();
            byte[] msgLen = new byte[4];
            Int32 bufferCnt = 0;

            System.Buffer.BlockCopy(buf, bufferCnt, msgLen, 0, msgLen.Length);
            int messageLength = BitConverter.ToInt32(msgLen, 0);
            bufferCnt += msgLen.Length;

            //if (messageLength == buf.Length)
            //{
                byte[] addressBytes = new byte[4];
                byte[] portBytes = new byte[sizeof(Int32)];
                byte[] cmdBytes = new byte[sizeof(Int32)];
                
                System.Buffer.BlockCopy(buf, bufferCnt, addressBytes, 0, addressBytes.Length);
                bufferCnt += addressBytes.Length;

                System.Buffer.BlockCopy(buf, bufferCnt, portBytes, 0, portBytes.Length);
                bufferCnt += portBytes.Length;

                System.Buffer.BlockCopy(buf, bufferCnt, cmdBytes, 0, cmdBytes.Length);
                bufferCnt += cmdBytes.Length;

                returnMsg.peerIP = new IPAddress(addressBytes);
                returnMsg.port = BitConverter.ToInt32(portBytes, 0);
                returnMsg.command = BitConverter.ToInt32(cmdBytes, 0);
            //}

            return returnMsg;
        }

        private commandMessage createCommandMessage(Int32 peerIndex, Int32 msgInt)
        {
            commandMessage returnMsg = new commandMessage();
            returnMsg.peerIP = peerList[peerIndex].peerIP;
            returnMsg.port = peerList[peerIndex].peerPort;
            returnMsg.command = msgInt;

            return returnMsg;
        }

        /// <summary>
        /// This method is invoked when an asycnhronous receive operation completes. If the 
        /// remote host closed the connection, then the socket is closed.  If data was received then
        /// the data is echoed back to the client.
        /// </summary>
        private void ProcessReceive(SocketAsyncEventArgs e)
        {
            Random randomNumberGenerator = new Random();

            // check if the remote host closed the connection
            AsyncUserToken token = (AsyncUserToken)e.UserToken;
            
            byte[] myBuffer = new byte[1501];
            System.Buffer.BlockCopy(e.Buffer, e.Offset, myBuffer, 0, e.Count);

            commandMessage msg = parseCommandMessage(myBuffer);
            int peerNumber;
            //create peer variable to send back to client
            commandMessage replyMsg = new commandMessage(); 

            //bug bug - do a real calc here
            int clientMsgStreamLength = 16;

            //copy to byte array
            byte[] intBytes = BitConverter.GetBytes(clientMsgStreamLength);
            byte[] addressBytes = new byte[4];
            byte[] portBytes = new byte[4];
            byte[] cmdBytes = new byte[4];
            
            switch (msg.command)
            {
                case 0:
                    peerInstance newPeer = new peerInstance();

                    newPeer.peerIP = msg.peerIP;
                    newPeer.peerPort = msg.port;

                    if (peerList.Count < 2)
                        peerNumber = 0;
                    else
                        peerNumber = randomNumberGenerator.Next(peerList.Count);

                    //add the peer to peerList
                    peerList.Add(new peerInstance());
                    int newPeerCnt = peerList.Count - 1;

                    peerList[newPeerCnt].peerIP = newPeer.peerIP;
                    peerList[newPeerCnt].peerPort = newPeer.peerPort;


                    intBytes = BitConverter.GetBytes(16);
                    addressBytes = peerList[peerNumber].peerIP.GetAddressBytes();
				    portBytes = BitConverter.GetBytes(peerList[peerNumber].peerPort);
					cmdBytes = BitConverter.GetBytes(0);

					System.Buffer.BlockCopy(intBytes, 0, myBuffer, 0, 4);  //prepends length to buffer
                    System.Buffer.BlockCopy(addressBytes, 0, myBuffer, 4, addressBytes.Length);
                    System.Buffer.BlockCopy(portBytes, 0, myBuffer, 4 + addressBytes.Length, portBytes.Length);
                    System.Buffer.BlockCopy(cmdBytes, 0, myBuffer, 4 + addressBytes.Length + portBytes.Length, cmdBytes.Length);
                    System.Buffer.BlockCopy(myBuffer, 0, e.Buffer, e.Offset, myBuffer.Length);
                    break;

                case 1:
                    replyMsg = msg;
                    replyMsg.command = 0;

                    intBytes = BitConverter.GetBytes(16);
                    addressBytes = replyMsg.peerIP.GetAddressBytes();
				    portBytes = BitConverter.GetBytes(replyMsg.port);
					cmdBytes = BitConverter.GetBytes(replyMsg.command);

					System.Buffer.BlockCopy(intBytes, 0, myBuffer, 0, 4);  //prepends length to buffer
                    System.Buffer.BlockCopy(addressBytes, 0, myBuffer, 4, addressBytes.Length);
                    System.Buffer.BlockCopy(portBytes, 0, myBuffer, 4 + addressBytes.Length, portBytes.Length);
                    System.Buffer.BlockCopy(cmdBytes, 0, myBuffer, 4 + addressBytes.Length + portBytes.Length, cmdBytes.Length);
                    System.Buffer.BlockCopy(myBuffer, 0, e.Buffer, e.Offset, myBuffer.Length);
                    break;
            }

            if (e.BytesTransferred > 0 && e.SocketError == SocketError.Success)
            {
                ////increment the count of the total bytes receive by the server
                //Interlocked.Add(ref m_totalBytesRead, e.BytesTransferred);
                //Console.WriteLine("The server has read a total of {0} bytes", m_totalBytesRead);

                //echo the data received back to the client
                bool willRaiseEvent = token.Socket.SendAsync(e);
                if (!willRaiseEvent)
                {
                    ProcessSend(e);
                }

            }
            else
            {
                CloseClientSocket(e);
            }
        }

        /// <summary>
        /// This method is invoked when an asynchronous send operation completes.  The method issues another receive
        /// on the socket to read any additional data sent from the client
        /// </summary>
        /// <param name="e"></param>
        private void ProcessSend(SocketAsyncEventArgs e)
        {
            if (e.SocketError == SocketError.Success)
            {
                // done echoing data back to the client
                AsyncUserToken token = (AsyncUserToken)e.UserToken;
                
                
                // read the next block of data send from the client
                bool willRaiseEvent = token.Socket.ReceiveAsync(e);
                if (!willRaiseEvent)
                {
                    ProcessReceive(e);
                }
            }
            else
            {
                CloseClientSocket(e);
            }
        }

        private void CloseClientSocket(SocketAsyncEventArgs e)
        {
            AsyncUserToken token = e.UserToken as AsyncUserToken;

            //token.m_socket.RemoteEndPoint.Address  & token.m_socket.RemoteEndPoint.Port is what need to be removed from peerList 

            // close the socket associated with the client
            try
            {
                token.Socket.Shutdown(SocketShutdown.Send);
            }
            // throws if client process has already closed
            catch (Exception) { }
            token.Socket.Close();

            // decrement the counter keeping track of the total number of clients connected to the server
            Interlocked.Decrement(ref m_numConnectedSockets);
            m_maxNumberAcceptedClients.Release();
            Console.WriteLine("A client has been disconnected from the server. There are {0} clients connected to the server", m_numConnectedSockets);

            // Free the SocketAsyncEventArg so they can be reused by another client
            m_readWritePool.Push(e);
        }

    }
}