'use client';

import { useState, useEffect, useCallback } from 'react';
import { create } from '@bufbuild/protobuf';
import { kvClient } from '@/lib/grpc';
import {
  PutRequestSchema,
  GetRequestSchema,
  DeleteRequestSchema,
  ListRequestSchema,
  StatsRequestSchema,
  HealthCheckRequestSchema,
  HealthCheckResponse_ServingStatus,
  type StatsResponse,
  type GetResponse,
  type DataEntry,
} from '@/proto/kv_pb';

const SERVING_STATUS_LABEL: Record<HealthCheckResponse_ServingStatus, string> = {
  [HealthCheckResponse_ServingStatus.UNKNOWN]: 'UNKNOWN',
  [HealthCheckResponse_ServingStatus.SERVING]: 'SERVING',
  [HealthCheckResponse_ServingStatus.NOT_SERVING]: 'NOT_SERVING',
};

function decodePayload(bytes: Uint8Array): string {
  try {
    return new TextDecoder('utf-8', { fatal: true }).decode(bytes);
  } catch {
    return `<binary, ${bytes.length} bytes>`;
  }
}

function formatUnixMs(value: bigint): string {
  const ms = Number(value);
  if (!Number.isFinite(ms) || ms === 0) return '—';
  return new Date(ms).toISOString();
}

export default function Dashboard() {
  const [namespace, setNamespace] = useState('default');
  const [id, setId] = useState('');
  const [data, setData] = useState('');

  const [getNs, setGetNs] = useState('default');
  const [getId, setGetId] = useState('');
  const [getResult, setGetResult] = useState<GetResponse | null>(null);
  const [getLoading, setGetLoading] = useState(false);

  const [deleteNs, setDeleteNs] = useState('default');
  const [deleteId, setDeleteId] = useState('');
  const [deleteLoading, setDeleteLoading] = useState(false);

  const [listNs, setListNs] = useState('default');
  const [listPageSize, setListPageSize] = useState('');
  const [listEntries, setListEntries] = useState<DataEntry[]>([]);
  const [listNextToken, setListNextToken] = useState('');
  const [listLoading, setListLoading] = useState(false);
  const [listFetched, setListFetched] = useState(false);

  const [stats, setStats] = useState<StatsResponse | null>(null);
  const [health, setHealth] = useState<HealthCheckResponse_ServingStatus>(
    HealthCheckResponse_ServingStatus.UNKNOWN,
  );
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const [statsResp, healthResp] = await Promise.all([
        kvClient.getStats(create(StatsRequestSchema)),
        kvClient.healthCheck(create(HealthCheckRequestSchema)),
      ]);
      setStats(statsResp);
      setHealth(healthResp.status);
    } catch (err) {
      console.error('Failed to refresh dashboard:', err);
      setHealth(HealthCheckResponse_ServingStatus.NOT_SERVING);
    }
  }, []);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 5000);
    return () => clearInterval(interval);
  }, [refresh]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!namespace || !id || !data) {
      setError('All fields are required');
      return;
    }

    setLoading(true);
    setError(null);
    setSuccess(null);

    try {
      const request = create(PutRequestSchema, {
        id,
        namespace,
        data: new TextEncoder().encode(data),
      });
      await kvClient.put(request);
      setSuccess(`Stored namespace="${namespace}", id="${id}"`);
      setId('');
      setData('');
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to store data');
    } finally {
      setLoading(false);
    }
  };

  const runGet = useCallback(
    async (ns: string, entryId: string) => {
      if (!ns || !entryId) {
        setError('All fields are required');
        return;
      }
      setGetLoading(true);
      setError(null);
      setSuccess(null);
      try {
        const response = await kvClient.get(
          create(GetRequestSchema, { id: entryId, namespace: ns }),
        );
        setGetResult(response);
        await refresh();
      } catch (err) {
        setGetResult(null);
        setError(err instanceof Error ? err.message : 'Failed to fetch entry');
      } finally {
        setGetLoading(false);
      }
    },
    [refresh],
  );

  const handleGet = async (e: React.FormEvent) => {
    e.preventDefault();
    await runGet(getNs, getId);
  };

  const runDelete = useCallback(
    async (ns: string, entryId: string) => {
      if (!ns || !entryId) {
        setError('All fields are required');
        return;
      }
      if (!window.confirm(`Delete ${ns}/${entryId}?`)) return;
      setDeleteLoading(true);
      setError(null);
      setSuccess(null);
      try {
        const response = await kvClient.delete(
          create(DeleteRequestSchema, { id: entryId, namespace: ns }),
        );
        setSuccess(
          response.deleted
            ? `Deleted ${ns}/${entryId}`
            : `No entry found for ${ns}/${entryId}`,
        );
        if (response.deleted) {
          setListEntries((prev) =>
            prev.filter((entry) => !(entry.namespace === ns && entry.id === entryId)),
          );
          if (getResult?.entry?.namespace === ns && getResult.entry.id === entryId) {
            setGetResult(null);
          }
        }
        await refresh();
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to delete entry');
      } finally {
        setDeleteLoading(false);
      }
    },
    [refresh, getResult],
  );

  const handleDelete = async (e: React.FormEvent) => {
    e.preventDefault();
    await runDelete(deleteNs, deleteId);
  };

  const runList = useCallback(
    async (loadMore: boolean) => {
      if (!listNs) {
        setError('Namespace is required');
        return;
      }
      const parsed = parseInt(listPageSize, 10);
      const pageSize = Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
      setListLoading(true);
      setError(null);
      setSuccess(null);
      try {
        const response = await kvClient.list(
          create(ListRequestSchema, {
            namespace: listNs,
            pageSize,
            pageToken: loadMore ? listNextToken : '',
          }),
        );
        setListEntries((prev) =>
          loadMore ? [...prev, ...response.entries] : response.entries,
        );
        setListNextToken(response.nextPageToken);
        setListFetched(true);
        await refresh();
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to list entries');
      } finally {
        setListLoading(false);
      }
    },
    [listNs, listPageSize, listNextToken, refresh],
  );

  const handleList = async (e: React.FormEvent) => {
    e.preventDefault();
    await runList(false);
  };

  const onListNsChange = (value: string) => {
    setListNs(value);
    setListEntries([]);
    setListNextToken('');
    setListFetched(false);
  };

  const viewFromList = async (entry: DataEntry) => {
    setGetNs(entry.namespace);
    setGetId(entry.id);
    await runGet(entry.namespace, entry.id);
  };

  const deleteFromList = async (entry: DataEntry) => {
    setDeleteNs(entry.namespace);
    setDeleteId(entry.id);
    await runDelete(entry.namespace, entry.id);
  };

  const healthLabel = SERVING_STATUS_LABEL[health];
  const healthClass = health === HealthCheckResponse_ServingStatus.SERVING
    ? 'healthy'
    : 'unhealthy';

  return (
    <div className="dashboard">
      <header>
        <h1>gRPC Dashboard</h1>
        <p className="subtitle">Key-Value Store Management</p>
      </header>

      <main>
        <section className="card">
          <h2>PUT Data</h2>
          <form onSubmit={handleSubmit}>
            <div className="form-group">
              <label htmlFor="namespace">Namespace</label>
              <input
                type="text"
                id="namespace"
                value={namespace}
                onChange={(e) => setNamespace(e.target.value)}
                placeholder="e.g., users"
                required
              />
            </div>

            <div className="form-group">
              <label htmlFor="id">ID</label>
              <input
                type="text"
                id="id"
                value={id}
                onChange={(e) => setId(e.target.value)}
                placeholder="e.g., user-123"
                required
              />
            </div>

            <div className="form-group">
              <label htmlFor="data">Data</label>
              <textarea
                id="data"
                value={data}
                onChange={(e) => setData(e.target.value)}
                placeholder="Enter your data here..."
                rows={6}
                required
              />
            </div>

            <button type="submit" disabled={loading}>
              {loading ? 'Storing...' : 'Store Data'}
            </button>
          </form>
        </section>

        <section className="card">
          <h2>GET Entry</h2>
          <form onSubmit={handleGet}>
            <div className="form-group">
              <label htmlFor="get-namespace">Namespace</label>
              <input
                type="text"
                id="get-namespace"
                value={getNs}
                onChange={(e) => setGetNs(e.target.value)}
                placeholder="e.g., users"
                required
              />
            </div>

            <div className="form-group">
              <label htmlFor="get-id">ID</label>
              <input
                type="text"
                id="get-id"
                value={getId}
                onChange={(e) => setGetId(e.target.value)}
                placeholder="e.g., user-123"
                required
              />
            </div>

            <button type="submit" disabled={getLoading}>
              {getLoading ? 'Fetching...' : 'Fetch'}
            </button>
          </form>

          {getResult && getResult.found === false && (
            <div className="entry-missing">Not found</div>
          )}

          {getResult && getResult.found && getResult.entry && (
            <div className="entry-view">
              <dl>
                <dt>Namespace</dt>
                <dd>{getResult.entry.namespace}</dd>
                <dt>ID</dt>
                <dd>{getResult.entry.id}</dd>
                <dt>Created</dt>
                <dd>{formatUnixMs(getResult.entry.createdAtUnixMs)}</dd>
                <dt>Updated</dt>
                <dd>{formatUnixMs(getResult.entry.updatedAtUnixMs)}</dd>
                <dt>Checksum</dt>
                <dd>{getResult.entry.checksum}</dd>
              </dl>
              <pre>{decodePayload(getResult.entry.data)}</pre>
            </div>
          )}
        </section>

        <section className="card">
          <h2>DELETE Entry</h2>
          <form onSubmit={handleDelete}>
            <div className="form-group">
              <label htmlFor="delete-namespace">Namespace</label>
              <input
                type="text"
                id="delete-namespace"
                value={deleteNs}
                onChange={(e) => setDeleteNs(e.target.value)}
                placeholder="e.g., users"
                required
              />
            </div>

            <div className="form-group">
              <label htmlFor="delete-id">ID</label>
              <input
                type="text"
                id="delete-id"
                value={deleteId}
                onChange={(e) => setDeleteId(e.target.value)}
                placeholder="e.g., user-123"
                required
              />
            </div>

            <button type="submit" disabled={deleteLoading}>
              {deleteLoading ? 'Deleting...' : 'Delete'}
            </button>
          </form>
        </section>

        <section className="card">
          <h2>LIST Entries</h2>
          <form onSubmit={handleList}>
            <div className="form-group">
              <label htmlFor="list-namespace">Namespace</label>
              <input
                type="text"
                id="list-namespace"
                value={listNs}
                onChange={(e) => onListNsChange(e.target.value)}
                placeholder="e.g., users"
                required
              />
            </div>

            <div className="form-group">
              <label htmlFor="list-page-size">Page size</label>
              <input
                type="number"
                id="list-page-size"
                min={0}
                value={listPageSize}
                onChange={(e) => setListPageSize(e.target.value)}
                placeholder="0 = server default"
              />
            </div>

            <button type="submit" disabled={listLoading}>
              {listLoading ? 'Listing...' : 'List'}
            </button>
          </form>

          {listFetched && listEntries.length === 0 && (
            <div className="entry-missing">No entries</div>
          )}

          {listEntries.length > 0 && (
            <ul className="entry-list">
              {listEntries.map((entry) => (
                <li key={`${entry.namespace} ${entry.id}`}>
                  <span>{entry.id}</span>
                  <span className="row-actions">
                    <button
                      type="button"
                      onClick={() => viewFromList(entry)}
                      disabled={getLoading}
                    >
                      View
                    </button>
                    <button
                      type="button"
                      onClick={() => deleteFromList(entry)}
                      disabled={deleteLoading}
                    >
                      Delete
                    </button>
                  </span>
                </li>
              ))}
            </ul>
          )}

          {listNextToken && (
            <button
              type="button"
              className="load-more"
              onClick={() => runList(true)}
              disabled={listLoading}
            >
              {listLoading ? 'Loading...' : 'Load More'}
            </button>
          )}
        </section>

        <section className="card">
          <h2>Health Status</h2>
          <div className="health-status">
            <span className={`status ${healthClass}`}>{healthLabel}</span>
          </div>
        </section>

        <section className="card">
          <h2>Statistics</h2>
          <div className="stats-grid">
            <div className="stat">
              <span className="stat-value">{stats?.readsTotal?.toString() ?? '0'}</span>
              <span className="stat-label">Total Reads</span>
            </div>
            <div className="stat">
              <span className="stat-value">{stats?.writesTotal?.toString() ?? '0'}</span>
              <span className="stat-label">Total Writes</span>
            </div>
            <div className="stat">
              <span className="stat-value">{stats?.deletesTotal?.toString() ?? '0'}</span>
              <span className="stat-label">Total Deletes</span>
            </div>
            <div className="stat">
              <span className="stat-value">{stats?.listOperationsTotal?.toString() ?? '0'}</span>
              <span className="stat-label">List Operations</span>
            </div>
          </div>
        </section>
      </main>

      {error && (
        <div className="alert error">
          {error}
          <button onClick={() => setError(null)} className="alert-close">&times;</button>
        </div>
      )}

      {success && (
        <div className="alert success">
          {success}
          <button onClick={() => setSuccess(null)} className="alert-close">&times;</button>
        </div>
      )}
    </div>
  );
}
