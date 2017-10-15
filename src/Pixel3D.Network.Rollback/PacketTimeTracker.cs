using System;
using System.Diagnostics;

namespace Pixel3D.Network.Rollback
{
    class PacketTimeTracker
    {

        /// <summary>The number of packets to keep timing information for</summary>
        public const int Capacity = 10;


        public double currentNetworkTime;
        public FrameDataBuffer<double> packetSentTimes = new FrameDataBuffer<double>();

        public int Count { get { return packetSentTimes.Count; } }

        public double[] allExpectedCurrentFrames = new double[Capacity];

        // The range of frame times received, discarding outliers
        public double NominalFrameRangeBegin { get; private set; }
        public double NominalFrameRangeEnd { get; private set; }



        /// <summary>The frame we would like to be the current frame (at this moment)</summary>
        public double DesiredCurrentFrame
        {
            get
            {
                // For most connections, the median frame should be suitable:
                return 0.5 * NominalFrameRangeBegin + 0.5 * NominalFrameRangeEnd;
            }
        }


        private const double safeRangeMinimumFrames = 2;

        /// <summary>The beginning of the range of frames we consider "close enough" to the desired frame</summary>
        public double SafeFrameRangeBegin
        {
            get
            {
                double desiredFrame = DesiredCurrentFrame;
                double nominalSize = desiredFrame - NominalFrameRangeBegin;
                double mirrorSize = NominalFrameRangeEnd - desiredFrame;
                return desiredFrame - Math.Max(Math.Max(nominalSize, mirrorSize), safeRangeMinimumFrames);
            }
        }

        /// <summary>The end of the range of frames we consider "close enough" to the desired frame</summary>
        public double SafeFrameRangeEnd
        {
            get
            {
                double desiredFrame = DesiredCurrentFrame;
                double nominalSize = NominalFrameRangeEnd - desiredFrame;
                double mirrorSize = desiredFrame - NominalFrameRangeBegin;
                return desiredFrame + Math.Max(Math.Max(nominalSize, mirrorSize), safeRangeMinimumFrames);
            }
        }




        /// <summary>
        /// Indicate to the timing code that a new packet was received
        /// </summary>
        /// <param name="packetFrame">The frame associated with a received packet</param>
        /// <param name="packetSentTime">The estimated local network time that the packet was sent at (in seconds)</param>
        public void ReceivePacket(int packetFrame, double packetSentTime)
        {
            // Once we are at capacity, ignore any packets that are too old (they'd be removed immediately anyway)
            if(packetSentTimes.Count >= Capacity && packetFrame < packetSentTimes.Keys[0])
                return;

            // Ignore any duplicate packets
            if(packetSentTimes.ContainsKey(packetFrame))
                return;


            packetSentTimes.Add(packetFrame, packetSentTime);

            if(packetSentTimes.Count > Capacity)
                packetSentTimes.RemoveAt(0);
        }


        /// <summary>
        /// Set a new network time and recalculate timing state
        /// </summary>
        /// <param name="currentNetworkTime">Network time in seconds</param>
        public void Update(double currentNetworkTime)
        {
            this.currentNetworkTime = currentNetworkTime;

            if(packetSentTimes.Count == 0)
                return; // Don't attempt to do any timing maths if we've got no data

            // Calculate the expected current frame for all packets:
            Debug.Assert(packetSentTimes.Count <= Capacity);
            for(int i = 0; i < packetSentTimes.Count; i++)
            {
                allExpectedCurrentFrames[i] = ExpectedCurrentFrameForPacket(packetSentTimes.Keys[i], packetSentTimes.Values[i]);
            }

            // Sort the expected frame numbers (for doing maths on them)
            Array.Sort(allExpectedCurrentFrames, 0, packetSentTimes.Count);
            int rangeBegin = 0;
            int rangeEnd = packetSentTimes.Count;

            // Discard the extreme values
            if(rangeEnd - rangeBegin > 4)
                rangeEnd--;
            if(rangeEnd - rangeBegin > 4)
                rangeBegin++;
            if(rangeEnd - rangeBegin > 6)
                rangeEnd--;
            if(rangeEnd - rangeBegin > 6)
                rangeBegin++;

            // Use this as the nominal range for the frame number
            NominalFrameRangeBegin = allExpectedCurrentFrames[rangeBegin];
            NominalFrameRangeEnd = allExpectedCurrentFrames[rangeEnd-1];
        }


        /// <summary>
        /// Calculate the expected current frame for a single packet.
        /// </summary>
        public double ExpectedCurrentFrameForPacket(int packetFrame, double packetSentTime)
        {
            double timeSincePacket = currentNetworkTime - packetSentTime;
            double framesSincePacket = timeSincePacket * RollbackDriver.FramesPerSecond;
            return packetFrame + framesSincePacket;
        }


    }
}
