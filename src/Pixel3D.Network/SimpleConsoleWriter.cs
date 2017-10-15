using Microsoft.Xna.Framework;
using Pixel3D.P2P.Diagnostics;

namespace Pixel3D.Network
{
    public class SimpleConsoleWriter : NetworkLogHandler
    {
        SimpleConsole target;

        public SimpleConsoleWriter(SimpleConsole target)
        {
            this.target = target;
        }

        public override void HandleLidgrenMessage(string message)
        {
            target.WriteLine(message, Color.DarkBlue);
        }

        public override void HandleMessage(string message)
        {
            target.WriteLine(message, Color.DarkGreen);
        }
    }
}
