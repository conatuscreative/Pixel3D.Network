using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using Lidgren.Network;

namespace Pixel3D.P2P
{
    internal class P2PClient : IPeerManager
    {
        internal readonly P2PNetwork owner;
        internal NetPeer NetPeer { get { return owner.netPeer; } }

        NetConnection p2pServer;
        public bool HasServer { get { return p2pServer != null; } }

        byte[] playerData;

        ulong sideChannelId;
        int sideChannelToken;
        string requestedPlayerName;


        public P2PClient(P2PNetwork owner, string playerName, byte[] playerData, ulong sideChannelId, int sideChannelToken)
        {
            this.owner = owner;
            this.requestedPlayerName = playerName.FilterName();
            this.playerData = playerData;
            this.sideChannelId = sideChannelId;
            this.sideChannelToken = sideChannelToken;

            owner.Log("P2P Client started");
        }



        /// <summary>List of all remote peers (no matter their local connection state). List contents are managed by the P2P server.</summary>
        SortedList<int, RemotePeer> allRemotePeers = new SortedList<int, RemotePeer>();



        #region Server Connect

        bool connectionStarted;

        public void ConnectImmediate(IPEndPoint p2pServerEndPoint)
        {
            connectionStarted = true;
            pendingVerifyAndConnect = null;

            NetPeer.Configuration.DisableMessageType(NetIncomingMessageType.UnconnectedData); // <- Gets re-enabled once we need it

            owner.Log("P2P Client is connecting to " + p2pServerEndPoint.ToString());

            var hailMessage = NetPeer.CreateMessage();
            hailMessage.Write((UInt16)(owner.appConfig.ApplicationVersion));
            hailMessage.WriteVariableUInt32((UInt32)owner.appConfig.ApplicationSignature.Length);
            hailMessage.Write(owner.appConfig.ApplicationSignature);
            IPAddress subnetMask;
            hailMessage.Write(new IPEndPoint(NetUtility.GetMyAddress(out subnetMask), NetPeer.Port));
            hailMessage.Write(sideChannelId);
            hailMessage.Write(sideChannelToken);
            hailMessage.Write(requestedPlayerName);
            hailMessage.WriteByteArray(playerData);

            p2pServer = NetPeer.Connect(p2pServerEndPoint, hailMessage);
            var serverPeer = new RemotePeer(p2pServer, null);
            Debug.Assert(ReferenceEquals(p2pServer.Tag, serverPeer)); // Assignment done in RemotePeer constructor
        }

        
        //
        // Side-channel verification before connection:
        //
        // This is a cheap and nasty way to attempt connection to multiple endpoints at the same time
        // because Lidgren can't handle that internally, and is too hard to modify to make it do so.
        //

        class PendingVerfiyAndConnect
        {
            public IPEndPoint p2pServerEndPoint;
            public int transmitCount;
            public double nextTransmitTime;
        }

        List<PendingVerfiyAndConnect> pendingVerifyAndConnect;


        public void TryToVerifyAndConnect(IPEndPoint p2pServerEndPoint)
        {
            if(connectionStarted)
                return;

            if(pendingVerifyAndConnect == null)
            {
                pendingVerifyAndConnect = new List<PendingVerfiyAndConnect>();
                NetPeer.Configuration.EnableMessageType(NetIncomingMessageType.UnconnectedData);
            }

            var pending = new PendingVerfiyAndConnect() { p2pServerEndPoint = p2pServerEndPoint };
            SendVerifyMessage(pending);
            pendingVerifyAndConnect.Add(pending);
        }

        private void SendVerifyMessage(PendingVerfiyAndConnect pending)
        {
            var message = NetPeer.CreateMessage();
            message.Write((byte)(UnconnectedMessage.SideChannelVerify));
            message.Write(sideChannelId);
            message.Write(sideChannelToken);
            NetPeer.SendUnconnectedMessage(message, pending.p2pServerEndPoint);

            pending.transmitCount++;
            pending.nextTransmitTime = NetTime.Now + ((pending.transmitCount * pending.transmitCount) * 0.5);
        }

        private void UpdateVerifyAndConnect()
        {
            if(connectionStarted || pendingVerifyAndConnect == null)
                return;
            var now = NetTime.Now;

            foreach(var pending in pendingVerifyAndConnect)
            {
                if(pending.transmitCount < 7 && pending.nextTransmitTime < now)
                    SendVerifyMessage(pending);
            }
        }

        private void HandleVerifyResponse(NetIncomingMessage message)
        {
            if(connectionStarted)
                return; // Can't do anything with this

            ulong sideChannelId;
            int token;
            try
            {
                sideChannelId = message.ReadUInt64();
                token = message.ReadInt32();
            }
            catch
            {
                return;
            }

            if(token == 0)
                return;

            if(sideChannelId == this.sideChannelId && token == this.sideChannelToken)
            {
                owner.Log("Receieved verify from " + message.SenderEndPoint);
                ConnectImmediate(message.SenderEndPoint);
            }
        }

        #endregion



        #region IPeerManager

        public void HandleMessage(NetIncomingMessage message, ref bool recycle)
        {
            Debug.Assert(!didDisconnect);

            RemotePeer senderRemotePeer = null;
            if(message.SenderConnection != null)
                senderRemotePeer = message.SenderConnection.Tag as RemotePeer;

            try
            {
                switch(message.MessageType)
                {
                    // Network management messages:
                    case NetIncomingMessageType.Data:
                        // (Check non-network-management messages aren't leaking through)
                        Debug.Assert(message.DeliveryMethod == NetDeliveryMethod.ReliableOrdered);
                        Debug.Assert(message.SequenceChannel == 0);
                        if(message.SenderConnection == p2pServer && p2pServer != null)
                            HandleNetworkManagementFromServer(message);
                        else
                            HandleNetworkManagementFromNonServer(message, ref recycle); // <- host migration
                        break;

                    // NAT Punch-through
                    case NetIncomingMessageType.UnconnectedData:
                        HandleUnconnectedMessage(message);
                        break;

                    // Incoming connection validation
                    case NetIncomingMessageType.ConnectionApproval:
                        HandleConnectionApproval(message);
                        break;

                    // Incomming connections:
                    case NetIncomingMessageType.StatusChanged:
                        NetConnectionStatus status = (NetConnectionStatus)message.ReadByte(); // <- NOTE: status change message generated inside Lidgren, no need to validate
                        switch(status)
                        {
                            case NetConnectionStatus.Connected:
                                HandlePeerConnected(message.SenderConnection);
                                break;
                            case NetConnectionStatus.Disconnected:
                                HandlePeerDisconnected(message.SenderConnection, message);
                                break;
                        }
                        break;
                }
            }
            catch(NetworkDataException exception)
            {
                NetworkDataError(senderRemotePeer, message.SenderConnection, exception);                    
            }
        }


        private void HandleUnconnectedMessage(NetIncomingMessage message)
        {
            if(message.LengthBits == 0)
                return;

            UnconnectedMessage um;
            try
            {
                um = (UnconnectedMessage)message.ReadByte();
            }
            catch
            {
                return; // Unexpected!
            }

            switch(um)
            {
                case UnconnectedMessage.NATPunchThrough:
                    HandlePunchThrough(message);
                    break;

                case UnconnectedMessage.SideChannelVerifyResponse:
                    HandleVerifyResponse(message);
                    break;

                default:
                    return; // Silently ignore message types we don't recognise
            }
        }


        public void Update()
        {
            Debug.Assert(!didDisconnect);

            if(!connectionStarted)
            {
                UpdateVerifyAndConnect();
            }
            else
            {
                UpdateHostMigration();
                if(owner.peerManager != this)
                    return; // Host migration happened

                UpdateUnconnectedRemotePeers();

                CheckAndReportInitialisedRTT();
            }
        }


        bool didDisconnect;

        public void HandleLocalDisconnection()
        {
            Debug.Assert(!didDisconnect);
            didDisconnect = true;

            DisableRemoteConnections();

            // Disconnect from everyone:
            DisconnectFromServer(); // (it's possible that the server is not in the remote connections list)
            foreach(var remotePeer in allRemotePeers.Values)
            {
                if(remotePeer.IsConnected)
                    remotePeer.Disconnect(DisconnectStrings.Leaving);
            }
        }


        public void KickDueToNetworkDataError(RemotePeer remotePeer)
        {
            Debug.Assert(remotePeer != null);

            if(remotePeer.PeerInfo.IsServer)
                owner.DisconnectAndThrow(UserVisibleStrings.ErrorInDataFromServer);
            else
                DisconnectRemoteClient(remotePeer, DisconnectStrings.DataError);
        }

        #endregion



        #region Network Data Errors

        private void NetworkDataError(RemotePeer remotePeer, NetConnection sender, Exception exception)
        {
            bool isServer = (sender != null && p2pServer != null && sender == p2pServer)
                    || (remotePeer != null && remotePeer.PeerInfo != null && remotePeer.PeerInfo.IsServer);

            // Note: PeerInfo can be null during startup
            string identifier = ((remotePeer != null && remotePeer.PeerInfo != null) ? remotePeer.PeerInfo.ToString() : (isServer ? "server" : "unknown peer"));

            owner.Log("Network data error from " + identifier);
            owner.LogException(exception);

            if(isServer)
            {
                owner.DisconnectAndThrow(UserVisibleStrings.ErrorInDataFromServer);
            }
            else
            {
                if(remotePeer != null)
                    DisconnectRemoteClient(remotePeer, DisconnectStrings.DataError);
            }
        }

        #endregion



        #region Network Management Commands from P2P Server

        private void HandleNetworkManagementFromServer(NetIncomingMessage message)
        {
            // Sanity check:
            Debug.Assert(message.MessageType == NetIncomingMessageType.Data);
            Debug.Assert(message.DeliveryMethod == NetDeliveryMethod.ReliableOrdered);
            Debug.Assert(message.SequenceChannel == 0);
            Debug.Assert(message.Position == 0);
            Debug.Assert(p2pServer != null);
            Debug.Assert(message.SenderConnection == p2pServer);

            var messageType = message.TryReadP2PServerMessage();
            if(!receivedNetworkStartInfo)
            {
                switch(messageType)
                {
                    case P2PServerMessage.NetworkStartInfo:
                        HandleNetworkStartInfo(message);
                        break;

                    default:
                        throw new ProtocolException("Bad network command: " + messageType);
                }
            }
            else
            {
                switch(messageType)
                {
                    case P2PServerMessage.PeerJoinedNetwork:
                        HandlePeerJoinedNetework(message);
                        break;
                    case P2PServerMessage.PeerLeftNetwork:
                        HandlePeerLeftNetwork(message);
                        break;
                    case P2PServerMessage.YouWereDisconnectedBy:
                        HandleYouWereDisconnectedBy(message);
                        break;
                    case P2PServerMessage.PeerBecameApplicationConnected:
                        HandlePeerBecameApplicationConnected(message);
                        break;

                    default:
                        throw new ProtocolException("Bad network command: " + messageType);
                }
            }
        }



        #region Network Startup

        bool receivedNetworkStartInfo = false;

        void HandleNetworkStartInfo(NetIncomingMessage message)
        {
            Debug.Assert(!receivedNetworkStartInfo); // should have been checked by caller
            Debug.Assert(message.SenderConnection == p2pServer);


            // Read from the network:
            GameInfo gameInfo;
            PeerInfo localPeerInfo;
            PeerInfo serverPeerInfo;
            PeerInfo[] otherConnections;
            int[] connectionTokens;
            try
            {
                gameInfo = new GameInfo(message);
                localPeerInfo = new PeerInfo(message, true, playerData);
                serverPeerInfo = new PeerInfo(message);

                int otherConnectionCount = message.ReadByte();
                otherConnections = new PeerInfo[otherConnectionCount];
                connectionTokens = new int[otherConnectionCount];
                for(int i = 0; i < otherConnectionCount; i++)
                {
                    otherConnections[i] = new PeerInfo(message);
                    connectionTokens[i] = message.ReadInt32();
                }
            }
            catch(Exception e) { throw new ProtocolException("Bad NetworkStartInfo message", e); }

            receivedNetworkStartInfo = true;


            // Apply received data:
            owner.GameInfo = gameInfo;
            owner.Log("Connecting to " + (gameInfo.IsInternetGame ? " Internet" : " LAN-only") + " game \"" + gameInfo.Name + "\"");
            owner.LocalPeerInfo = localPeerInfo;
            owner.Log("I am " + localPeerInfo.ToString());

            // Note: The server is given a RemotePeer at startup with no PeerInfo, so that there is
            //       somewhere to queue up any messages received on channels other than ReliableOrdered/0,
            //       before NetworkStartInfo arrives.
            RemotePeer serverRemotePeer = p2pServer.Tag as RemotePeer;
            Debug.Assert(serverRemotePeer != null);
            serverRemotePeer.SetPeerInfo(serverPeerInfo);
            allRemotePeers.Add(serverPeerInfo.ConnectionId, serverRemotePeer);
            owner.Log("P2P Server is " + serverPeerInfo.ToString());

            for(int i = 0; i < otherConnections.Length; i++)
            {
                owner.Log("Connect to " + otherConnections[i].ToString());
                ConnectToRemote(otherConnections[i], connectionTokens[i], true);
            }
        }

        #endregion


        #region Connect and Disconnect Commands

        private void HandlePeerJoinedNetework(NetIncomingMessage message)
        {
            PeerInfo peerInfo;
            int connectionToken;
            try
            {
                peerInfo = new PeerInfo(message);
                connectionToken = message.ReadInt32();
            }
            catch(Exception e) { throw new ProtocolException("Bad PeerJoinedNetework message", e); }

            owner.Log("Joining network: " + peerInfo.ToString());
            ConnectToRemote(peerInfo, connectionToken, false);
        }

        private void HandlePeerLeftNetwork(NetIncomingMessage message)
        {
            int connectionId;
            try
            {
                connectionId = message.ReadInt32();
            }
            catch(Exception e) { throw new ProtocolException("Bad PeerLeftNetwork message", e); }


            // Find the relevant connection:
            RemotePeer remotePeer;
            if(!allRemotePeers.TryGetValue(connectionId, out remotePeer))
            {
                throw new ProtocolException("P2P Server asked to disconnect unknown peer #" + connectionId);
            }

            owner.Log("Leaving network: " + remotePeer.PeerInfo);

            // If they're still unconnected, stop trying to connect to them:
            for(int i = unconnectedRemotePeers.Count - 1; i >= 0; i--)
            {
                if(unconnectedRemotePeers[i].remotePeer == remotePeer)
                {
                    unconnectedRemotePeers.RemoveAt(i);
                    break;
                }
            }

            // And if they were connected, disconnect them:
            if(remotePeer.IsConnected)
            {
                remotePeer.Disconnect(DisconnectStrings.DisconnectedByServer);
            }

            // If they were application-connected, remove them:
            if(remotePeer.PeerInfo.IsApplicationConnected)
            {
                BecomeApplicationDisconnected(remotePeer, message);
            }

            // Finally, remove them:
            allRemotePeers.Remove(connectionId);
        }

        #endregion


        #region "You Were Disconnected"

        private void HandleYouWereDisconnectedBy(NetIncomingMessage message)
        {
            int reporterId;
            try
            {
                reporterId = message.ReadInt32();
            }
            catch(Exception e) { throw new ProtocolException("Bad YouWereDisconnectedBy message", e); }

            owner.Log("Disputing disconnection by #" + reporterId); // (not bothering to look up the PeerInfo for this)

            // p2pServer should already have been checked for validity by our caller. Sanity check because we're about to send to it.
            Debug.Assert(p2pServer != null && p2pServer == message.SenderConnection);

            // Just bounce it back to the server, indicating we are alive...
            var reponse = NetPeer.CreateMessage();
            reponse.Write(P2PClientMessage.DisputeDisconnection);
            reponse.Write(reporterId);
            p2pServer.SendMessage(reponse, NetDeliveryMethod.ReliableOrdered, 0);
        }

        #endregion


        #region Peer Became Application Connected

        private void HandlePeerBecameApplicationConnected(NetIncomingMessage message)
        {
            int connectionId;
            try
            {
                connectionId = message.ReadInt32();
            }
            catch(Exception e) { throw new ProtocolException("Bad PeerBecameApplicationConnected message", e); }

            if(connectionId == owner.LocalPeerInfo.ConnectionId)
            {
                LocalBecomeApplicationConnected(message);
            }
            else
            {
                RemotePeer remotePeer;
                if(!allRemotePeers.TryGetValue(connectionId, out remotePeer))
                {
                    throw new ProtocolException("Received PeerBecameApplicationConnected message for unknown peer #" + connectionId);
                }

                BecomeApplicationConnected(remotePeer, message);
            }
        }

        #endregion

        #endregion



        #region Server Disconnect

        void DisconnectFromServer()
        {
            if(p2pServer != null)
            {
                RemotePeer serverRemotePeer = (p2pServer.Tag as RemotePeer);
                if(serverRemotePeer != null)
                    serverRemotePeer.Disconnect(DisconnectStrings.Leaving);
                else
                    p2pServer.Disconnect(DisconnectStrings.Leaving);
                p2pServer = null;
            }

            // If the server goes away, no point in making new connections
            ClearUnconnectedRemotePeers();
        }

        #endregion



        #region Unconnnected Peers

        /// <summary>List of pending remote peer connections.</summary>
        List<UnconnectedRemotePeer> unconnectedRemotePeers = new List<UnconnectedRemotePeer>();

        void UpdateUnconnectedRemotePeers()
        {
            double now = NetTime.Now;

            for(int i = unconnectedRemotePeers.Count - 1; i >= 0; i--) // Reverse iterate to allow removal
            {
                if(!unconnectedRemotePeers[i].Update(now))
                {
                    owner.Log(unconnectedRemotePeers[i].PeerInfo.ToString() + " timed out waiting to connect");
                    ReportPeerDisconnected(unconnectedRemotePeers[i].PeerInfo);
                    unconnectedRemotePeers.RemoveAt(i);
                }
            }

            // If there's no unconnected peers left, disable handling of incoming connections in Lidgren:
            if(unconnectedRemotePeers.Count == 0)
            {
                DisableRemoteConnections();
            }
        }

        void ConnectToRemote(PeerInfo peerInfo, int connectionToken, bool initiateConnection)
        {
            // Check we don't already have this connection:
            if(allRemotePeers.ContainsKey(peerInfo.ConnectionId))
            {
                throw new ProtocolException("Received duplicate connection request for existing connection: " + allRemotePeers[peerInfo.ConnectionId].PeerInfo);
            }

            RemotePeer remotePeer = new RemotePeer(null, peerInfo);
            allRemotePeers.Add(peerInfo.ConnectionId, remotePeer);

            EnableRemoteConnections();

            // This will start the connection attempt
            unconnectedRemotePeers.Add(new UnconnectedRemotePeer(this, remotePeer, connectionToken, initiateConnection));
        }


        void EnableRemoteConnections()
        {
            // We could diferentiate between outgoing and incoming connections, if we were being picky
            NetPeer.Configuration.EnableMessageType(NetIncomingMessageType.UnconnectedData);
            NetPeer.Configuration.EnableMessageType(NetIncomingMessageType.ConnectionApproval);
            NetPeer.Configuration.AcceptIncomingConnections = true;
            // NOTE: There's possibly *technically* a threading bug in the above. Lidgren reads Configuration directly on the network thread without locking.
        }

        void DisableRemoteConnections()
        {
            NetPeer.Configuration.AcceptIncomingConnections = false;
            NetPeer.Configuration.DisableMessageType(NetIncomingMessageType.UnconnectedData);
            NetPeer.Configuration.DisableMessageType(NetIncomingMessageType.ConnectionApproval);
        }

        void ClearUnconnectedRemotePeers()
        {
            unconnectedRemotePeers.Clear();
            DisableRemoteConnections();
        }


        void SetConnection(RemotePeer remotePeer, NetConnection connection)
        {
            remotePeer.AddConnection(connection);

            if(remotePeer.PeerInfo.IsApplicationConnected)
                ApplicationConnectedRemotePeerReceivedConnetion(remotePeer);
        }


        void HandlePunchThrough(NetIncomingMessage message)
        {
            int localId, remoteId, connectionToken;
            try
            {
                UnconnectedRemotePeer.ReadConnectionMessage(message, out localId, out remoteId, out connectionToken);
            }
            catch
            {
                owner.UnconnectedProtocolError("Received bad punch message");
                return; // Silently ignore bad punch message
            }

            if(owner.LocalPeerInfo == null || localId != owner.LocalPeerInfo.ConnectionId)
                return; // Bad local ID (silently ignore - possibly stale connection attempt, if we reconnected)

            // Search through the unconnected peers to find a match:
            for(int i = unconnectedRemotePeers.Count - 1; i >= 0; i--)
            {
                if(unconnectedRemotePeers[i].PeerInfo.ConnectionId == remoteId
                        && unconnectedRemotePeers[i].connectionToken == connectionToken)
                {
                    // If we are the connection initiator, make the connection:
                    if(unconnectedRemotePeers[i].initiateConnection)
                    {
                        var hailMessage = NetPeer.CreateMessage();
                        UnconnectedRemotePeer.WriteConnectionMessage(hailMessage, localId, remoteId, connectionToken);
                        NetConnection connection = NetPeer.Connect(message.SenderEndPoint, hailMessage);

                        SetConnection(unconnectedRemotePeers[i].remotePeer, connection);
                        unconnectedRemotePeers.RemoveAt(i);
                    }

                    // If the other party is initiating, it gets handled in HandleConnectionApproval

                    // Either way, because we found the match, don't need to keep searching:
                    return;
                }
            }

            // If we get to here, we couldn't find the matching unconnected peer
            // Silently ignore - probably stale connection attempt (got cancelled by server or we've already started connecting to it)
        }

        void HandleConnectionApproval(NetIncomingMessage message)
        {
            int localId, remoteId, connectionToken;
            try
            {
                UnconnectedRemotePeer.ReadConnectionMessage(message, out localId, out remoteId, out connectionToken);
            }
            catch
            {
                owner.UnconnectedProtocolError("Denying bad connection approval request!");
                message.SenderConnection.Deny();
                return;
            }

            if(localId != owner.LocalPeerInfo.ConnectionId)
            {
                message.SenderConnection.Deny();
                return; // Bad local ID (silently ignore - possibly stale connection attempt, if we reconnected)
            }

            // Search through the unconnected peers to find a match:
            for(int i = unconnectedRemotePeers.Count - 1; i >= 0; i--)
            {
                if(unconnectedRemotePeers[i].PeerInfo.ConnectionId == remoteId
                        && unconnectedRemotePeers[i].connectionToken == connectionToken)
                {
                    if(unconnectedRemotePeers[i].initiateConnection) // We are initiating the connection
                    {
                        owner.UnconnectedProtocolError("Denying connection attempt, wrong direction!"); // Don't call us, we'll call you
                        message.SenderConnection.Deny();
                        return;
                    }

                    // Approve the connection and set it on the connected client:
                    message.SenderConnection.Approve();
                    SetConnection(unconnectedRemotePeers[i].remotePeer, message.SenderConnection);
                    unconnectedRemotePeers.RemoveAt(i);
                    return;
                }
            }

            // If we get to here, we don't have an entry for this connecting party, so ditch them:
            message.SenderConnection.Deny();
        }


        #endregion



        #region Remote Peer Connect and Disconnect

        private void HandlePeerConnected(NetConnection connection)
        {
            Debug.Assert(connection != null);

            // The P2P server connection is handled externally
            if(connection == p2pServer)
                return;

            RemotePeer remotePeer = connection.Tag as RemotePeer;
            if(remotePeer == null)
            {
                // We really should not get a connection we don't know about (should have been approved first)
                Debug.Assert(false); // If this triggers, it indicates a programming error (not a protocol error)
                connection.Disconnect(DisconnectStrings.InternalError);
                return;
            }

            // Sanity check
            Debug.Assert(allRemotePeers.ContainsKey(remotePeer.PeerInfo.ConnectionId));
            Debug.Assert(allRemotePeers[remotePeer.PeerInfo.ConnectionId] == remotePeer);

            // Report connection success:
            ReportPeerConnected(remotePeer.PeerInfo);
        }


        
        private void HandleServerDisconnected(string reason)
        {
            owner.Log("Disconnected from server, with reason: " + reason);

            RemotePeer remotePeer = p2pServer.Tag as RemotePeer;
            if(remotePeer == null)
                owner.Log("(Server connection had no associated remote peer)");
            else if(remotePeer.PeerInfo == null)
                owner.Log("(Server connection had no peer info set)");
            else
                owner.Log("(Server was: " + remotePeer.PeerInfo + ")");


            DisconnectFromServer();

            // Try to host-migrate:
            if(owner.LocalPeerInfo != null && owner.LocalPeerInfo.IsApplicationConnected)
            {
                Debug.Assert(owner.GameInfo != null && receivedNetworkStartInfo);

                if(reason == DisconnectStrings.Shutdown)
                {
                    hostMigrationValidatedByServer = true;
                    TriggerHostMigration();
                    return;
                }

                if(reason == DisconnectStrings.LidgrenTimedOut && !HasHostMigrationInvalidation)
                {
                    TriggerHostMigration();
                    return;
                }
            }


            // If we get to here, then host migration is invalid
            // (We weren't app-connected, got kicked, or something weird happened)
            owner.DisconnectAndThrow(DisconnectStrings.LocaliseServerDisconnectionReason(reason));
        }


        private void HandlePeerDisconnected(NetConnection connection, NetIncomingMessage message)
        {
            Debug.Assert(connection != null);
            // NOTE: This method does not remove the RemotePeer from the list of RemotePeers
            //       That list is managed by the server (even if we lose a connection, the server may think we still have it)

            string reason = message.ReadString(); // <- NOTE: message is generated inside Lidgren, no need to validate

            if(connection == p2pServer)
            {
                HandleServerDisconnected(reason);
                return;
            }

            RemotePeer remotePeer = connection.Tag as RemotePeer;
            if(remotePeer == null)
                return; // Probably because we disconnected already (by server request)

            remotePeer.WasDisconnected();

            owner.Log("Disconnected from " + remotePeer.PeerInfo + ", with reason: " + reason);
            ReportPeerDisconnected(remotePeer.PeerInfo);

            clientDisconnections.AddDisconnection(reason);
            if(hostMigrationPending && HasHostMigrationInvalidation)
            {
                InvalidateHostMigration();
                return;
            }
        }


        private void DisconnectRemoteClient(RemotePeer remotePeer, string reason)
        {
            Debug.Assert(remotePeer.PeerInfo.IsServer == false); // Disconnecting the server means a full network disconnection

            owner.Log("Disconnecting " + remotePeer.PeerInfo + " with reason: " + reason);
            remotePeer.Disconnect(reason);
            ReportPeerDisconnected(remotePeer.PeerInfo);
        }

        #endregion



        #region Reporting to Server

        private void ReportPeerConnected(PeerInfo peerInfo)
        {
            if(p2pServer != null)
            {
                owner.Log("Reporting connected: " + peerInfo);
                var message = NetPeer.CreateMessage();
                message.Write(P2PClientMessage.Connected);
                message.Write(peerInfo.ConnectionId);
                p2pServer.SendMessage(message, NetDeliveryMethod.ReliableOrdered, 0);
            }
        }

        private void ReportPeerDisconnected(PeerInfo peerInfo)
        {
            if(p2pServer != null)
            {
                owner.Log("Reporting disconnected: " + peerInfo);
                var message = NetPeer.CreateMessage();
                message.Write(P2PClientMessage.Disconnected);
                message.Write(peerInfo.ConnectionId);
                p2pServer.SendMessage(message, NetDeliveryMethod.ReliableOrdered, 0);
            }
        }


        bool hasReportedRTTInitialised = false;

        private void CheckAndReportInitialisedRTT()
        {
            if(hasReportedRTTInitialised)
                return; // already done

            if(p2pServer != null && p2pServer.AverageRoundtripTime >= 0)
            {
                var message = owner.CreateMessage();
                message.Write(P2PClientMessage.RTTInitialised);
                p2pServer.SendMessage(message, NetDeliveryMethod.ReliableOrdered, 0);
                hasReportedRTTInitialised = true;
            }
        }

        #endregion



        #region Application-connected peers

        void LocalBecomeApplicationConnected(NetIncomingMessage message)
        {
            if(owner.LocalPeerInfo.IsApplicationConnected)
            {
                throw new ProtocolException("Received application-connected message for self, while already application-connected");
            }

            owner.LocalPeerInfo.IsApplicationConnected = true;
            owner.Log("Local became application-connected");

            // Expose all of the already-application-connected peers:
            foreach(var remotePeer in allRemotePeers.Values)
            {
                if(remotePeer.PeerInfo.IsApplicationConnected)
                    owner.AddApplicationConnection(remotePeer);
            }


            owner.NetworkApplication.ConnectedOnClient(message);
        }


        void BecomeApplicationConnected(RemotePeer remotePeer, NetIncomingMessage message)
        {
            if(remotePeer.PeerInfo.IsApplicationConnected)
            {
                throw new ProtocolException("Received application-connected message for " + remotePeer.PeerInfo + ", who is already application-connected");
            }

            remotePeer.PeerInfo.IsApplicationConnected = true;
            owner.Log("Application Connected: " + remotePeer.PeerInfo);

            // If we're application connected, expose new connections to the application:
            if(owner.LocalPeerInfo.IsApplicationConnected)
            {
                owner.AddApplicationConnection(remotePeer);
                owner.NetworkApplication.JoinOnClient(remotePeer, message);
            }
        }


        void ApplicationConnectedRemotePeerReceivedConnetion(RemotePeer remotePeer)
        {
            if(owner.LocalPeerInfo.IsApplicationConnected)
            {
                // The extremely weird situation where we're already application-connected,
                // and a client who is supposedly application-connected connects to us.
                //
                // In theory this shouldn't ever happen. If we are application-connected, other
                // clients need to form a connection to us, and we need to report it to the server,
                // before the server should let them become application-connected.
                owner.Log("Connected to " + remotePeer.PeerInfo + " even though we are both marked as application-connected!");
                Debug.Assert(false);

                // Do our best to handle it anyway:
                owner.RemoveApplicationConnection(remotePeer);
                owner.AddApplicationConnection(remotePeer);
            }
        }


        void BecomeApplicationDisconnected(RemotePeer remotePeer, NetIncomingMessage message)
        {
            Debug.Assert(remotePeer.PeerInfo.IsApplicationConnected); // should have been checked earlier

            remotePeer.PeerInfo.IsApplicationConnected = false;
            owner.Log("Application Disconnected: " + remotePeer.PeerInfo);

            // If we're application connected, expose disconnections to the application:
            if(owner.LocalPeerInfo.IsApplicationConnected)
            {
                owner.RemoveApplicationConnection(remotePeer);
                owner.NetworkApplication.LeaveOnClient(remotePeer, message);
            }
        }

        #endregion



        #region Host Migration - Detection

        bool hostMigrationPending;
        double hostMigrationTriggerTime;
        const double hostMigrationTimeOut = 40; // seconds (must be > Lidgren time-out time)


        void TriggerHostMigration()
        {
            hostMigrationPending = true;
            hostMigrationTriggerTime = NetTime.Now;

            owner.Log("Host migration triggered");

            AttemptHostMigration();
        }


        void UpdateHostMigration()
        {
            clientDisconnections.UpdateOnClient(this);

            if(!hostMigrationPending)
                return; // nothing to do

            if(HasHostMigrationInvalidation)
            {
                InvalidateHostMigration();
                return;
            }

            if(NetTime.Now > hostMigrationTriggerTime + hostMigrationTimeOut)
            {
                owner.DisconnectAndThrow(UserVisibleStrings.HostMigrationTimedOut);
            }

            AttemptHostMigration();
        }


        /// <summary>Get the connection ID of the peer with host migration priority, or -1 if none is available</summary>
        int FindHostMigrationPriority()
        {
            int localConnectionId = owner.LocalPeerInfo.ConnectionId;
            bool localSupportsHostMigration = true;

            // Find a suitable remote peer with the lowest connection ID
            // Assumes that allRemotePeers is sorted by connection id
            for(int i = 0; i < allRemotePeers.Values.Count; i++)
            {
                RemotePeer remotePeer = allRemotePeers.Values[i];

                // If we can host, skip over any peers that come after us
                if(localSupportsHostMigration && remotePeer.PeerInfo.ConnectionId > localConnectionId)
                    break;

                if(!remotePeer.PeerInfo.IsServer && remotePeer.IsConnected && remotePeer.PeerInfo.IsApplicationConnected)
                {
                    return remotePeer.PeerInfo.ConnectionId;
                }
            }

            if(localSupportsHostMigration)
                return localConnectionId;

            return -1;
        }


        void AttemptHostMigration()
        {
            int hostWithPriority = FindHostMigrationPriority();

            if(hostWithPriority == -1) // No host migration target
            {
                owner.DisconnectAndThrow(UserVisibleStrings.DisconnectedFromServerNoHostMigrationTarget);
                return;
            }

            if(hostWithPriority == owner.LocalPeerInfo.ConnectionId)
            {
                BecomeHost();
                return;
            }

            RemotePeer newHostPeer = allRemotePeers[hostWithPriority];
            if(newHostPeer.hostMigrationQueue != null)
            {
                ReplayHostMigrationQueue(newHostPeer);
            }
        }

        #endregion



        #region Host Migration - Validation

        ClientDisconnections clientDisconnections = new ClientDisconnections();

        bool hostMigrationValidatedByServer = false;

        bool HasHostMigrationInvalidation
        {
            get
            {
                if(hostMigrationValidatedByServer)
                    return false;

                // If we've recieved a "Disconnected by server" message recently, migration is invalid
                if(clientDisconnections.HasDisconnectedByServerDisconnects)
                    return true;

                // If the entire network timed out at once, we've probably lost network connection
                // Detect by seeing if we have zero live connections at at least one recent time-out
                if(clientDisconnections.HasTimedOutDisconnects)
                {
                    foreach(var remotePeer in allRemotePeers.Values)
                    {
                        if(remotePeer.IsConnected)
                            return false; // There's still someone connected
                    }
                    return true;
                }

                return false;
            }
        }


        void InvalidateHostMigration()
        {
            owner.Log("Host migration invalidated");
            owner.DisconnectAndThrow(UserVisibleStrings.ServerTimedOut); // Treat as the original time-out
        }

        #endregion



        #region Host Migration - Sending / Become Host

        void BecomeHost()
        {
            int maxConnectionId = owner.LocalPeerInfo.ConnectionId;

            // Disconnect from all remotes who are not application connected (they can't be recovered by P2PServer)
            foreach(var remotePeer in allRemotePeers.Values)
            {
                if(!remotePeer.PeerInfo.IsApplicationConnected)
                {
                    if(remotePeer.IsConnected)
                        remotePeer.Disconnect(DisconnectStrings.BecomingHost);
                }
                else
                {
                    if(remotePeer.PeerInfo.ConnectionId > maxConnectionId)
                        maxConnectionId = remotePeer.PeerInfo.ConnectionId;
                }
            }

            P2PServer newServer = new P2PServer(owner, hostMigrationValidatedByServer, maxConnectionId);
            owner.peerManager = newServer;
            newServer.CompleteHostMigration();

            hostMigrationPending = false;
            didDisconnect = true; // Invalidate this IPeerManager
        }

        #endregion



        #region Host Migration - Receiving / Changing Host


        private void HandleNetworkManagementFromNonServer(NetIncomingMessage message, ref bool recycle)
        {
            // Sanity check:
            Debug.Assert(message.MessageType == NetIncomingMessageType.Data);
            Debug.Assert(message.DeliveryMethod == NetDeliveryMethod.ReliableOrdered);
            Debug.Assert(message.SequenceChannel == 0);
            Debug.Assert(message.Position == 0);
            Debug.Assert(message.SenderConnection != null);

            RemotePeer remotePeer = message.SenderConnection.Tag as RemotePeer;
            if(remotePeer == null)
                return; // Shouldn't happen? (but I don't trust Lidgren)
            Debug.Assert(allRemotePeers.ContainsKey(remotePeer.PeerInfo.ConnectionId));


            P2PServerMessage m = message.TryPeekP2PServerMessage();

            if(m == P2PServerMessage.HostMigration)
            {
                if(remotePeer.hostMigrationQueue != null)
                    throw new ProtocolException("Received excess host migration message");

                // If we can host-migrate immediately, do so.
                if(hostMigrationPending && FindHostMigrationPriority() == remotePeer.PeerInfo.ConnectionId)
                {
                    HandleHostMigrationMessage(remotePeer, message);
                }
                else // Otherwise, queue it for later
                {
                    remotePeer.hostMigrationQueue = new Queue<NetIncomingMessage>();
                    remotePeer.hostMigrationQueue.Enqueue(message);
                    recycle = false;
                }
            }
            else if(m == P2PServerMessage.Unknown)
            {
                throw new ProtocolException("Received unknown server message");
            }
            else // All other server messages
            {
                if(remotePeer.hostMigrationQueue == null)
                    throw new ProtocolException("Received unexpected server message from a client");
                if(remotePeer.hostMigrationQueue.Count > 250) // Absurd number of messages received
                    throw new ProtocolException("Host migration queue overflow");

                remotePeer.hostMigrationQueue.Enqueue(message);
                recycle = false;
            }
        }


        private void HandleHostMigrationMessage(RemotePeer newHostPeer, NetIncomingMessage message)
        {
            Debug.Assert(p2pServer == null);
            Debug.Assert(receivedNetworkStartInfo && owner.LocalPeerInfo != null);

            owner.Log("Reading host migration message from " + newHostPeer.PeerInfo);

            var m = message.TryReadP2PServerMessage();
            Debug.Assert(m == P2PServerMessage.HostMigration); // All paths to this method should have already checked this

            List<PeerInfo> replacementPeerInfos;
            try
            {
                bool foundSelf = false;

                int remotePeerCount = message.ReadByte();
                replacementPeerInfos = new List<PeerInfo>(remotePeerCount);
                for(int i = 0; i < remotePeerCount; i++)
                {
                    PeerInfo peerInfo = new PeerInfo(message);
                    replacementPeerInfos.Add(peerInfo);

                    if(peerInfo.ConnectionId == owner.LocalPeerInfo.ConnectionId)
                        foundSelf = true;

                    if(peerInfo.IsServer || !peerInfo.IsApplicationConnected || peerInfo.ConnectionId == newHostPeer.PeerInfo.ConnectionId)
                        throw new ProtocolException("Host migration peer list has invalid peer info data");
                }

                if(foundSelf == false)
                    throw new ProtocolException("Host migration peer list did not include the local peer");
            }
            catch(Exception e)
            {
                throw new ProtocolException("Bad host migration message", e);
            }


            // At this point, the new server is accepted:
            owner.Log("Accepting host migration from " + newHostPeer.PeerInfo);
            p2pServer = newHostPeer.Connection;
            hostMigrationPending = false;
            Debug.Assert(newHostPeer.Connection != null && newHostPeer.Connection == message.SenderConnection);
            Debug.Assert(newHostPeer.PeerInfo.IsApplicationConnected);

            // Set server info (required by error-handling)
            foreach(var remotePeer in allRemotePeers.Values)
                remotePeer.PeerInfo.IsServer = false;
            newHostPeer.PeerInfo.IsServer = true;

            

            // Inform the new server that we accepted them:
            var response = NetPeer.CreateMessage();
            response.Write(P2PClientMessage.AcceptHostMigration);
            p2pServer.SendMessage(response, NetDeliveryMethod.ReliableOrdered, 0);


            // Sync our peer list with the incoming host:
            SortedList<int, RemotePeer> replacementRemotePeers = new SortedList<int, RemotePeer>();


            // The new host does not broadcast itself, but we can accept it implicitly:
            replacementRemotePeers.Add(newHostPeer.PeerInfo.ConnectionId, newHostPeer);
            allRemotePeers.Remove(newHostPeer.PeerInfo.ConnectionId); // Take from this list

            foreach(var peerInfo in replacementPeerInfos)
            {
                int connectionId = peerInfo.ConnectionId;
                RemotePeer existingRemotePeer;

                if(connectionId == owner.LocalPeerInfo.ConnectionId)
                {
                    owner.LocalPeerInfo = peerInfo;
                }
                else if(allRemotePeers.TryGetValue(connectionId, out existingRemotePeer))
                {
                    // NOTE: we trust the values from the new server match the original, except these values which are implicit anyway:
                    existingRemotePeer.PeerInfo.IsApplicationConnected = true;
                    replacementRemotePeers.Add(connectionId, existingRemotePeer);
                    allRemotePeers.Remove(connectionId); // Take from this list
                }
                else
                {
                    replacementRemotePeers.Add(connectionId, new RemotePeer(null, peerInfo)); // these guys won't last long
                }
            }

            // Any peers left in the previous server's peer list did not survive the host migration, must be disconnected before we drop their references:
            foreach(var remotePeer in allRemotePeers.Values)
            {
                DisconnectRemoteClient(remotePeer, DisconnectStrings.DisconnectedByServer);
            }

            // Replace all the old lists:
            allRemotePeers = replacementRemotePeers;
            owner.ClearApplicationConnections();
            foreach(var remotePeer in allRemotePeers.Values)
                owner.AddApplicationConnection(remotePeer);


            owner.NetworkApplication.HostMigrationChangeHost(newHostPeer, message);


            // Any peers in the new remote peer list without a connection need to be reported as such to the server
            // TODO: Do something about the excess disconnection report for the original server?
            foreach(var remotePeer in allRemotePeers.Values)
            {
                if(!remotePeer.IsConnected)
                    ReportPeerDisconnected(remotePeer.PeerInfo);
            }
        }


        private void ReplayHostMigrationQueue(RemotePeer newHostPeer)
        {
            // First message is always host-migration
            NetIncomingMessage message = newHostPeer.hostMigrationQueue.Dequeue();
            try
            {
                HandleHostMigrationMessage(newHostPeer, message);
            }
            catch(NetworkDataException exception)
            {
                NetworkDataError(newHostPeer, message.SenderConnection, exception);
            }
            NetPeer.Recycle(message);


            if(p2pServer == null) // Host migration failed
            {
                Debug.Assert(!newHostPeer.IsConnected); // Failed peer should have been disconnected
                return;
            }
            else // Host migration succeeded
            {
                Debug.Assert(p2pServer == newHostPeer.Connection);

                // Unwind all their queued host messages:
                while(newHostPeer.hostMigrationQueue.Count > 0)
                {
                    message = newHostPeer.hostMigrationQueue.Dequeue();
                    try
                    {
                        HandleNetworkManagementFromServer(message);
                    }
                    catch(NetworkDataException exception)
                    {
                        NetworkDataError(newHostPeer, message.SenderConnection, exception);
                    }
                    NetPeer.Recycle(message);
                }
            }
        }


        #endregion

    }
}
