using System.Collections.Generic;
using Lidgren.Network;

namespace Pixel3D.P2P
{
    /// <summary>
    /// Track client disconnection reasons, for host migration.
    /// Normally disconnection reasons are untrusted, but at host-migration time, we have to take what we can get.
    /// </summary>
    class ClientDisconnections
    {
        /// <summary>List of "Disconnected by server" messages and their associated times.</summary>
        List<double> disconnectedByServerTimes = new List<double>();

        /// <summary>List of time-outs and their associated times.</summary>
        List<double> timedOutTimes = new List<double>();


        /// <summary>How long to buffer disconnect messages, in seconds</summary>
        public const double clientBufferTime = 30;

        

        public void UpdateOnClient(P2PClient owner)
        {
            if(!owner.HasServer)
                return; // Don't expire any messages while we're waiting on a new server.

            double expireTime = NetTime.Now - clientBufferTime;

            while(disconnectedByServerTimes.Count > 0 && disconnectedByServerTimes[0] < expireTime)
                disconnectedByServerTimes.RemoveAt(0);

            while(timedOutTimes.Count > 0 && timedOutTimes[0] < expireTime)
                timedOutTimes.RemoveAt(0);
        }


        
        public void AddDisconnection(string reason)
        {
            double now = NetTime.Now;
            
            if(reason == DisconnectStrings.DisconnectedByServer)
                disconnectedByServerTimes.Add(now);
            
            if(reason == DisconnectStrings.LidgrenTimedOut)
                timedOutTimes.Add(now);
        }


        public bool HasDisconnectedByServerDisconnects
        {
            get { return disconnectedByServerTimes.Count > 0; }
        }

        public bool HasTimedOutDisconnects
        {
            get { return timedOutTimes.Count > 0; }
        }
        
    }
}
