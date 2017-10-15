using System;

namespace Pixel3D.P2P
{
    /// <summary>
    /// Thrown when the network gets disconnected
    /// </summary>
    [Serializable]
    public class NetworkDisconnectionException : Exception
    {
        public NetworkDisconnectionException() { }
        public NetworkDisconnectionException(string message) : base(message) { }
        public NetworkDisconnectionException(string message, Exception inner) : base(message, inner) { }
        protected NetworkDisconnectionException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context)
            : base(info, context) { }
    }
}
