using System;

namespace MsSqlHelper.Interfaces
{
    public interface ITransactionalRepository : IDisposable
    {
        void Commit();
        void Rollback();
    }
}
