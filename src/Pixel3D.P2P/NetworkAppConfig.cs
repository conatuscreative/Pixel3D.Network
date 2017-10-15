using System;

namespace Pixel3D.P2P
{
    public class NetworkAppConfig
    {
        public const int ApplicationSignatureMaximumLength = 32;

        /// <param name="appId">A very short string to identify the application</param>
        /// <param name="knownPorts">List of ports used by the application</param>
        /// <param name="version">The protocol version of the application. NOTE: Application is responsible for bumping this is the P2P layer protocol changes!</param>
        /// <param name="signature">Signature of the application (for version compatibility check)</param>
        public NetworkAppConfig(string appId, int[] knownPorts, ushort version, byte[] signature)
        {
            if(knownPorts.Length == 0)
                throw new ArgumentOutOfRangeException("knownPorts", "Must specify at least one known port");

            if(signature == null)
                signature = new byte[0];

            if(signature.Length > ApplicationSignatureMaximumLength)
                throw new ArgumentException("Application signature too long", "signature");


            this.AppId = appId;
            this.KnownPorts = knownPorts;
            this.ApplicationVersion = version;
            this.ApplicationSignature = signature;
        }

        internal int[] KnownPorts { get; private set; }

        internal string AppId { get; private set; }
        internal ushort ApplicationVersion { get; private set; }
        internal byte[] ApplicationSignature { get; private set; }

        
    }
}
