using System.Diagnostics;
using Lidgren.Network;

namespace Pixel3D.P2P.Diagnostics
{
    public abstract class NetworkLogHandler
    {
        public abstract void HandleLidgrenMessage(string message);
        public abstract void HandleMessage(string message);


        internal void InternalReceiveMessage(NetIncomingMessage message)
        {
            // Require that the message is not read yet
            Debug.Assert(message.Position == 0);

            switch(message.MessageType)
            {
                case NetIncomingMessageType.Error: // "should never happen" according to Lidgren
                    HandleLidgrenMessage("UNKNOWN MESSAGE TYPE!");
                    break;


                case NetIncomingMessageType.VerboseDebugMessage:
                    HandleLidgrenMessage("VERBOSE: " + message.PeekString());
                    break;
                case NetIncomingMessageType.DebugMessage:
                    HandleLidgrenMessage("DEBUG: " + message.PeekString());
                    break;
                case NetIncomingMessageType.WarningMessage:
                    HandleLidgrenMessage("WARNING: " + message.PeekString());
                    break;
                case NetIncomingMessageType.ErrorMessage:
                    HandleLidgrenMessage("ERROR: " + message.PeekString());
                    break;


                case NetIncomingMessageType.StatusChanged:
                    NetConnectionStatus status = (NetConnectionStatus)message.PeekByte();
                    var sender = message.SenderEndPoint;
                    HandleLidgrenMessage("STATUS: " + status + ((sender != null) ? (" (" + sender.ToString() + ")") : ""));
                    break;
            }
        }
    }
}
