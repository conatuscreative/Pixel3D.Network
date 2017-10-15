namespace Pixel3D.Network.Rollback
{
    public interface IAcceptsDesyncDumps
    {
        void ExportComparativeDesyncDump(byte[] lastGoodSnapshot, byte[] localSnapshot, byte[] remoteSnapshot);

        void ExportSimpleDesyncFrame(byte[] localSnapshot);
    }
}
