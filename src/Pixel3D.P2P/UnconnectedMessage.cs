namespace Pixel3D.P2P
{
    /// <summary>
    /// NOTE: a zero-length unconnected message is used for non-responsive NAT punches
    /// </summary>
    enum UnconnectedMessage : byte
    {
        /// <summary>This one is used for forming connections between P2P clients.</summary>
        NATPunchThrough,

        SideChannelVerify,

        SideChannelVerifyResponse,
    }
}
