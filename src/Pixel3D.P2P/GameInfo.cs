using Lidgren.Network;

namespace Pixel3D.P2P
{
    public class GameInfo
    {
        public string Name { get; private set; }
        public bool IsInternetGame { get; private set; }
        public bool SideChannelAuth { get; private set; }

        public GameInfo(string name, bool isInternetGame, bool sideChannelAuth)
        {
            this.Name = name.FilterName();
            this.IsInternetGame = isInternetGame;
            this.SideChannelAuth = sideChannelAuth;
        }
        
        internal void CopyFrom(GameInfo other)
        {
            this.Name = other.Name;
            this.IsInternetGame = other.IsInternetGame;
            this.SideChannelAuth = other.SideChannelAuth;
        }


        #region Network Read/Write

        // Used by "dud" discovered game
        internal GameInfo(string name)
        {
            Name = name;
        }

        internal GameInfo(NetIncomingMessage message)
        {
            Name = message.ReadString().FilterName();
            IsInternetGame = message.ReadBoolean();
            SideChannelAuth = message.ReadBoolean();
        }

        internal void WriteTo(NetOutgoingMessage message)
        {
            message.Write(Name.FilterName());
            message.Write(IsInternetGame);
            message.Write(SideChannelAuth);
        }

        #endregion

    }
}
