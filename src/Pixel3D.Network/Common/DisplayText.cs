using System;
using System.Text;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;

namespace Pixel3D.Network.Common
{
    public class DisplayText : IDisposable
    {
        SpriteBatch sb;
        Texture2D whitePixel;
        SpriteFont font;
        Color background;

        Vector2 position;

        public DisplayText(GraphicsDevice device, SpriteFont font, Color background)
        {
            this.sb = new SpriteBatch(device);
            this.font = font;
            this.background = background;

            whitePixel = new Texture2D(sb.GraphicsDevice, 1, 1);
            whitePixel.SetData(new[] { Color.White });
        }

        public void Dispose()
        {
            sb.Dispose();
            whitePixel.Dispose();
        }


        public void SetPosition(Vector2 position)
        {
            this.position = position;
        }


        public GraphicsDevice GraphicsDevice { get { return sb.GraphicsDevice; } }

        public void Begin()
        {
            Begin(Vector2.Zero);
        }

        public void Begin(Vector2 position)
        {
            sb.Begin();
            this.position = position;
        }

        public void End()
        {
            sb.End();
        }


        #region Write Line:

        public void WriteLine(StringBuilder text, Color color)
        {
            // Background
            Vector2 size = font.MeasureString(text);
            sb.Draw(whitePixel, position - new Vector2(2, 0), null, background, 0, Vector2.Zero, size + new Vector2(4, 0), 0, 0);

            // Text
            sb.DrawString(font, text, position, color);
            position.Y += font.LineSpacing;
        }        

        public void WriteLine(StringBuilder text)
        {
            WriteLine(text, Color.Black);
        }

        public void WriteLine(string text, Color color)
        {
            // Background
            Vector2 size = font.MeasureString(text);
            sb.Draw(whitePixel, position - new Vector2(2, 0), null, background, 0, Vector2.Zero, size + new Vector2(4, 0), 0, 0);

            // Text
            sb.DrawString(font, text, position, color);
            position.Y += font.LineSpacing;
        }

        public void WriteLine(string text)
        {
            WriteLine(text, Color.Black);
        }

        public void WriteLine()
        {
            position.Y += font.LineSpacing;
        }

        #endregion


    }
}
