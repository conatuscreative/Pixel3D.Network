#if WINDOWS // FIXME: wut -flibit
#define USE_FIXUP_XNA_CLOCK
#endif

using System;
using System.Collections.Generic;
#if USE_FIXUP_XNA_CLOCK
using Timer=System.Windows.Forms.Timer;
#endif
using System.Reflection;
using Microsoft.Xna.Framework;
using System.Threading;

namespace Pixel3D.Network
{
    public class FixupXNAClock
    {
        // See: http://gamedev.stackexchange.com/q/51376/288

        Game game;
#if USE_FIXUP_XNA_CLOCK
        Timer timer;
#endif

        public FixupXNAClock(Game game)
        {
            this.game = game;
            
#if USE_FIXUP_XNA_CLOCK

            // Break the chain of Suspend and Resume events
            // Internally to XNA 4.0, these cause form resize begin/end to stop/start the game clock
            // (and so normally preventing time from accumulating while the form is being moved or resized)
            // Disabling this behaviour causes resize (and move) to be handled like other actions that
            // stop the form going idle (eg: clicking the title bar, bringing up the alt+space menu)
            object host = typeof(Game).GetField("host", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(game);
            host.GetType().BaseType.GetField("Suspend", BindingFlags.NonPublic | BindingFlags.Instance).SetValue(host, null);
            host.GetType().BaseType.GetField("Resume", BindingFlags.NonPublic | BindingFlags.Instance).SetValue(host, null);

            // Prevent re-entrance (can happen when a Debug.Assert dialog comes up, the timer will merrily start firing from the dialog's WinProc)
            // Normally you shouldn't rely on the way event handler invocations are ordered. But given that we're touching the underlying multi-cast delegate directly with reflection...
            var originalIdleHandler = (EventHandler<EventArgs>)host.GetType().BaseType.GetField("Idle", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(host);

            Delegate newHandler = Delegate.Combine(
                    new EventHandler<EventArgs>(EnterXnaTick),
                    originalIdleHandler,
                    new EventHandler<EventArgs>(ExitXnaTick));

            host.GetType().BaseType.GetField("Idle", BindingFlags.NonPublic | BindingFlags.Instance).SetValue(host, newHandler);


            timer = new Timer();
            timer.Interval = (int)(game.TargetElapsedTime.TotalMilliseconds);
            timer.Tick += new EventHandler(timer_Tick);
            // The timer only starts operation after the first tick (prevents re-entrance between our construction and the game actually starting to tick)

#endif
        }

#if USE_FIXUP_XNA_CLOCK

        // NOTE: I've stopped the timer from stopping and starting itself, and instead just let it run continuiously,
        //       and ignore timer messages when we don't need them. This is because there is at least one user report
        //       (on 3 Windows 10 machines) that credibly suggests that on Windows 10, the regular WinForms timer will
        //       leak handles if repeatedly started and stopped.
        //          See: http://steamcommunity.com/app/422810/discussions/0/135511455868932661/


        bool started = false;
        int canTick = 0; // <- I'm not convinced that Interlocked is required here (the message loop should be single-threaded)... but oh well...

        void EnterXnaTick(object sender, EventArgs args)
        {
            //timer.Stop();
            Interlocked.Increment(ref canTick);
        }

        void ExitXnaTick(object sender, EventArgs args)
        {
            //timer.Start();
            Interlocked.Decrement(ref canTick);

            if(!started)
            {
                started = true;
                timer.Start();
            }
        }


        bool manualTick;
        int manualTickCount = 0;
        void timer_Tick(object sender, EventArgs e)
        {
            if(isStopped)
                return;

            if(Interlocked.CompareExchange(ref canTick, 1, 0) == 0)
            {
                if(manualTickCount > 2)
                {
                    manualTick = true;
                    game.Tick();
                    manualTick = false;
                }

                manualTickCount++;

                Interlocked.Decrement(ref canTick);
            }
        }

#endif

        /// <summary>Call at the start of Game.Update</summary>
        public void Update()
        {
#if USE_FIXUP_XNA_CLOCK
            if(!manualTick)
                manualTickCount = 0;
#endif
        }


        bool isStopped = false;

        public void EmergencyStop()
        {
#if USE_FIXUP_XNA_CLOCK
            isStopped = true; // <- This is reliable.
            timer.Stop(); // <- This is only to play nice. It's possible for the timer to get restarted by an Idle event (I think -AR).
#endif
        }

    }
}
