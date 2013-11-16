using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;


namespace CentralServer
{
    [Serializable()]
    class peerInstance
    {
        private IPAddress _peerIP;
        //private string _peerHostname;
        private Int32 _peerPort;

        public IPAddress peerIP
        {
            get { return _peerIP; }
            set { _peerIP = value; }
        }

        //public string peerHostname
        //{
        //    get { return _peerHostname; }
        //    set { _peerHostname = value; }
        //}

        public Int32 peerPort
        {
            get { return _peerPort; }
            set { _peerPort = value; }
        }
    }
}
