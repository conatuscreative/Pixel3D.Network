using System;
using System.Diagnostics;

namespace Pixel3D.P2P
{
    /// <summary>
    /// Represents an unrecoverable error that occurs when reading a NetIncomingMessage.
    /// 
    /// Any method that dispatches NetIncomingMessages, whether it is directly from Lidgren
    /// or a message queued in a RemotePeer, should catch and handle this exception.
    /// </summary>
    [Serializable]
    public class NetworkDataException : Exception
    {
        public NetworkDataException() { }
        public NetworkDataException(string message) : base(message) { }
        public NetworkDataException(string message, Exception inner) : base(message, inner) { }
        protected NetworkDataException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context)
            : base(info, context) { }
    }



    /// <summary>
    /// Represents a protocol error (ie: badly formed packet).
    /// In debug mode, with trusted peers, should never happen unless there's a programming error - so break into the debugger.
    /// But could theoretically be caused by a remote client with a modified game.
    /// </summary>
    [Serializable]
    public class ProtocolException : NetworkDataException
    {
        public ProtocolException() { Debug.Assert(false); }
        public ProtocolException(string message) : base(message) { Debug.Assert(false); }
        public ProtocolException(string message, Exception inner) : base(message, inner) { Debug.Assert(false); }
        protected ProtocolException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context)
            : base(info, context) { Debug.Assert(false); }
    }


    [Serializable]
    public class InsufficientDataToContinueException : Exception
    {
        public InsufficientDataToContinueException() { }
        public InsufficientDataToContinueException(string message) : base(message) { }
        public InsufficientDataToContinueException(string message, Exception inner) : base(message, inner) { }
        protected InsufficientDataToContinueException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context)
            : base(info, context) { }
    }
}

