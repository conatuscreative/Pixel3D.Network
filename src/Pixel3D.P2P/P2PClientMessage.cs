using Lidgren.Network;

namespace Pixel3D.P2P
{
    /// <summary>Messages sent from the P2P client to the P2P server</summary>
    enum P2PClientMessage
    {
        /// <summary>
        /// Connected to a given peer.
        /// </summary>
        Connected,


        /// <summary>
        /// Failed to connect to a given peer or they were disconnected from us.
        /// </summary>
        Disconnected,

        /// <summary>
        /// If another peer says they've disconnected from us, and we're the "older"
        /// of the two peers, the server will give us a chance to say we're still alive.
        /// </summary>
        DisputeDisconnection,


        /// <summary>
        /// In what I would consider to be a bug in Lidgren, the RTT is not initialised when a connection is first established.
        /// So instead we make it a requirement at the P2P layer to become application-connected.
        /// </summary>
        RTTInitialised,


        /// <summary>
        /// Indicate to a recently-host-migrated sever that their host migration request was accepted.
        /// </summary>
        AcceptHostMigration,


        Unknown,
    }


    static class P2PClientMessageExtensions
    {
        public static void Write(this NetOutgoingMessage message, P2PClientMessage m)
        {
            message.Write((byte)m);
        }

        public static P2PClientMessage TryReadP2PClientMessage(this NetIncomingMessage message)
        {
            try
            {
                return (P2PClientMessage)message.ReadByte();
            }
            catch
            {
                return P2PClientMessage.Unknown;
            }
        }
    }
}
