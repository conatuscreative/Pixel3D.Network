using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using Pixel3D.Network.P2P;
using Lidgren.Network;

namespace Pixel3D.P2P
{
    public class DiscoveredGame
    {
        public GameInfo GameInfo { get; private set; }


        /// <summary>The server where the game is being hosted (unique identifier for discovered servers).</summary>
        public IPEndPoint EndPoint { get; private set; }

        private string hostString;
        public string HostString
        {
            get { return hostString ?? (hostString = EndPoint == null ? null : EndPoint.Address.ToString()); }
        }

        internal const double DiscoveryResponseTimeout = 30; // seconds
        internal double expireTime;

        /// <summary>0 = unknown mismatch, -1 = too old, 1 = too new.</summary>
        public int? VersionMismatch { get; internal set; }

        public bool IsFull { get; internal set; }


        /// <summary>Arbitrary data provided by the application</summary>
        public byte[] applicationData;


        public string StatusString
        {
            get
            {
                if(VersionMismatch.HasValue)
                {
                    if(VersionMismatch.GetValueOrDefault() < 0)
                        return "(version too old)";
                    else if(VersionMismatch.GetValueOrDefault() > 0)
                        return "(version too new)";
                    else
                        return "(version mismatch)";
                }
                else if(IsFull)
                    return "(full)";
                else
                    return string.Empty;
            }
        }

        public bool CanJoin { get { return !VersionMismatch.HasValue && !IsFull; } }



        internal void CopyFrom(DiscoveredGame other)
        {
            this.GameInfo.CopyFrom(other.GameInfo);

            this.EndPoint = other.EndPoint;
            this.expireTime = other.expireTime;
            this.VersionMismatch = other.VersionMismatch;
            this.IsFull = other.IsFull;
            this.applicationData = other.applicationData; // <- assumed to be immutable
        }

        private DiscoveredGame() { }

        public static DiscoveredGame FakeGame()
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            var random = new Random();
            var name = new string(Enumerable.Repeat(chars, 14).Select(s => s[random.Next(s.Length)]).ToArray());
            var game = new DiscoveredGame();
            game.GameInfo = new GameInfo(name, false, false);
            game.EndPoint = new IPEndPoint(IPAddress.Parse("255.255.255.255"), 65535);
            game.IsFull = true;
            return game;
        }

        internal static void WriteDiscoveryResponse(NetworkAppConfig appConfig, NetOutgoingMessage message, GameInfo gameInfo, bool isFull, MemoryStream applicationData)
        {   
            // START: Version safe data
            // {
            message.Write(appConfig.AppId);
            message.Write(gameInfo.Name.FilterName());
            message.Write((UInt16)(appConfig.ApplicationVersion));
            message.WriteVariableUInt32((UInt32)appConfig.ApplicationSignature.Length);
            message.Write(appConfig.ApplicationSignature);
            // }
            // END: Version safe data

            message.Write(gameInfo.IsInternetGame);
            message.Write(isFull);
            message.WriteMemoryStreamAsByteArray(applicationData); // <- fill in for WriteByteArray
        }



        /// <param name="messageForTiming">Used only for timing and source data, not read from</param>
        private DiscoveredGame(NetIncomingMessage messageForTiming)
        {
            expireTime = messageForTiming.ReceiveTime + DiscoveryResponseTimeout;
            EndPoint = messageForTiming.SenderEndPoint;
            if(EndPoint == null)
                throw new InvalidOperationException();
        }

        public static DiscoveredGame ReadFromDiscoveryResponse(NetworkAppConfig appConfig, NetIncomingMessage message)
        {
            try
            {
                // START: Version safe data
                // {

                string theirAppId = message.ReadString();
                if(theirAppId != appConfig.AppId)
                    return null; // Wrong application

                string gameName = message.ReadString().FilterName();

                UInt16 theirAppVersion = message.ReadUInt16();
                int theirAppSignatureLength = (int)message.ReadVariableUInt32();
                
                byte[] theirAppSignature;
                if(theirAppSignatureLength < 0 || theirAppSignatureLength > NetworkAppConfig.ApplicationSignatureMaximumLength)
                    theirAppSignature = null;
                else
                    theirAppSignature = message.ReadBytes(theirAppSignatureLength);

                // }
                // END: Version safe data


                // Check for version mismatch
                if(theirAppVersion != appConfig.ApplicationVersion || theirAppSignature == null || !appConfig.ApplicationSignature.SequenceEqual(theirAppSignature))
                {
                    GameInfo gi = new GameInfo(gameName);
                    DiscoveredGame dg = new DiscoveredGame(messageForTiming: message);
                    dg.GameInfo = gi;

                    if(theirAppVersion < appConfig.ApplicationVersion)
                        dg.VersionMismatch = -1;
                    else if(theirAppVersion > appConfig.ApplicationVersion)
                        dg.VersionMismatch = 1;
                    else
                        dg.VersionMismatch = 0;

                    Debug.Assert(dg.VersionMismatch.HasValue);

                    return dg;
                }

                // If we get here, all the versioning is correct:
                {
                    bool isInternetGame = message.ReadBoolean();
                    GameInfo gi = new GameInfo(gameName, isInternetGame, false); // <- NOTE: we are assuming that the side-channel handles discovery

                    DiscoveredGame dg = new DiscoveredGame(messageForTiming: message);
                    dg.GameInfo = gi;
                    dg.IsFull = message.ReadBoolean();
                    dg.applicationData = message.ReadByteArray();
                    return dg;
                }
            }
            catch
            {
                return null;
            }
        }
    }
}
