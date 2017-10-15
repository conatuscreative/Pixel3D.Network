using System;
using System.Collections.Generic;
using System.Diagnostics;
using Lidgren.Network;

namespace Pixel3D.P2P
{
    public class Discovery
    {
        #region Internal Interface

        P2PNetwork owner;
        NetPeer NetPeer { get { return owner.netPeer; } }
        int[] discoveryPorts;


        internal Discovery(P2PNetwork owner, int[] discoveryPorts)
        {
            Debug.Assert(owner.Discovery == null);

            this.owner = owner;
            this.discoveryPorts = discoveryPorts;

            discoveryList = new List<DiscoveredGame>();
            Items = discoveryList.AsReadOnly();

            NetPeer.Configuration.EnableMessageType(NetIncomingMessageType.DiscoveryResponse);
        }


        internal void Stop()
        {
            Debug.Assert(owner.Discovery != null);

            NetPeer.Configuration.DisableMessageType(NetIncomingMessageType.DiscoveryResponse);
        }


        const double timeBetweenPolls = 5; // seconds
        double nextPollTime = 0; // poll network on first update

        internal void Update()
        {
            double now = NetTime.Now;

            if(nextPollTime < now)
            {
                nextPollTime = now + timeBetweenPolls;

                // Clear out any expired games in the list:
                for(int i = discoveryList.Count - 1; i >= 0; i--)
                {
                    if(discoveryList[i].expireTime < now)
                    {
                        discoveryList.RemoveAt(i);
                        discoveryListDirty = true;
                    }
                }

                // Broadcast a discovery packet
                foreach(int port in discoveryPorts)
                {
                    NetPeer.DiscoverLocalPeers(port);
                }
            }


            if(discoveryListDirty)
            {
                discoveryListDirty = false;
                if(OnItemsChanged != null)
                    OnItemsChanged(this);
            }
        }


        private bool discoveryListDirty = false;

        private List<DiscoveredGame> discoveryList;



        internal void ReceiveDiscoveryResponse(NetIncomingMessage message)
        {
            Debug.Assert(message.SenderEndPoint != null);

            // Read game info:
            DiscoveredGame discoveredGame = DiscoveredGame.ReadFromDiscoveryResponse(owner.appConfig, message);
            if(discoveredGame == null) // not a valid response
                return;

            // Check if we need to update an existing game:
            // (RemoteGameInfo could be versioned, if we were worried about packet ordering)
            for(int i = 0; i < discoveryList.Count; i++)
            {
                if(discoveryList[i].EndPoint.Equals(discoveredGame.EndPoint))
                {
                    discoveryList[i].CopyFrom(discoveredGame);
                    discoveryListDirty = true;
                    return;
                }
            }

            // Otherwise just add it:
            discoveryList.Add(discoveredGame);
            discoveryListDirty = true;
        }

        #endregion


        #region Public Interface

        public IList<DiscoveredGame> Items { get; private set; }

        public event Action<Discovery> OnItemsChanged;

        #endregion

    }
}
