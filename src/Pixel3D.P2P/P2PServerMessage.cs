using Lidgren.Network;

namespace Pixel3D.P2P
{
    /// <summary>Messages from the P2P server to the P2P client</summary>
    enum P2PServerMessage
    {
        /// <summary>
        /// Initial data for <see cref="P2PClient"/> to connect to the P2P network
        /// </summary>
        NetworkStartInfo,

        /// <summary>
        /// Another client has joined the network. Perform NAT punchthrough to that peer and allow them to connect.
        /// </summary>
        PeerJoinedNetwork,

        /// <summary>
        /// Another client has left the network. Disconnect them.
        /// </summary>
        PeerLeftNetwork,

        /// <summary>
        /// Another client on the network reported you disconnected. Are you still alive?
        /// </summary>
        YouWereDisconnectedBy,

        /// <summary>
        /// A peer just became connected to the network, connect them at the application layer.
        /// </summary>
        PeerBecameApplicationConnected,

        /// <summary>
        /// We just became the server due to host migration
        /// </summary>
        HostMigration,


        Unknown,
    }


    static class P2PServerMessageExtensions
    {
        public static P2PServerMessage Validate(this P2PServerMessage m)
        {
            if(m < 0 || m > P2PServerMessage.Unknown)
                return P2PServerMessage.Unknown;
            return m;
        }

        public static void Write(this NetOutgoingMessage message, P2PServerMessage m)
        {
            message.Write((byte)m);
        }

        public static P2PServerMessage TryReadP2PServerMessage(this NetIncomingMessage message)
        {
            try
            {
                return ((P2PServerMessage)message.ReadByte()).Validate();
            }
            catch
            {
                return P2PServerMessage.Unknown;
            }
        }

        public static P2PServerMessage TryPeekP2PServerMessage(this NetIncomingMessage message)
        {
            try
            {
                return ((P2PServerMessage)message.PeekByte()).Validate();
            }
            catch
            {
                return P2PServerMessage.Unknown;
            }
        }

    }
}
