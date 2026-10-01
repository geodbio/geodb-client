"""
The geoDB Open Exploration Protocol Python client.

    import geodb
    gx = geodb.Client(token="gdbg_...", base_url="https://api.geodb.io")
    df = gx.collars().to_dataframe()

One project per grant: a ``Client`` authenticates with a project-pinned,
read-only access grant (``Authorization: Grant <token>``) and every call is
scoped to that one project. Knox tokens work too (``auth_scheme="Token"``) for
first-party use.
"""

import time

import requests

from .errors import AuthError, NotFoundError, APIError, ExportError, WriteRefused
from .writes import WriteResult, rows_from

__all__ = ["Client"]

_DEFAULT_TIMEOUT = 60
_DEFAULT_BASE = "https://api.geodb.io"
_DEFAULT_PREFIX = "/api/v2"


class Paginated:
    """A lazy iterator over a v2 LimitOffset endpoint.

    Iterate for records; call :meth:`to_dataframe` to pull everything into a
    pandas DataFrame. Follows ``next`` links until the page set is exhausted.
    """

    def __init__(self, client, path, params=None, page_size=500):
        self._client = client
        self._path = path
        self._params = dict(params or {})
        self._params.setdefault("limit", page_size)

    def __iter__(self):
        params = dict(self._params)
        url = self._client._url(self._path)
        first = True
        while url:
            data = self._client._get_url(url, params=params if first else None)
            first = False
            if isinstance(data, dict) and "results" in data:
                for row in data["results"]:
                    yield row
                url = data.get("next")
            else:  # non-paginated payload (shouldn't happen on v2 list endpoints)
                for row in (data if isinstance(data, list) else [data]):
                    yield row
                url = None

    def to_dataframe(self):
        """Materialise all records into a :class:`pandas.DataFrame`."""
        import pandas as pd
        return pd.DataFrame(list(self))

    def count(self):
        """Total matching records (a single HEAD-ish page read)."""
        data = self._client._get(self._path, params={**self._params, "limit": 1})
        return data.get("count") if isinstance(data, dict) else None


class Asset:
    """A STAC asset — download it via the authenticated short-SAS redirect."""

    def __init__(self, client, key, data):
        self.key = key
        self.href = data.get("href")
        self.type = data.get("type")
        self.roles = data.get("roles", [])
        self.title = data.get("title")
        self.size = data.get("file:size")
        self._client = client
        self._raw = data

    def download(self, path, chunk_size=1 << 20):
        """Stream this asset to ``path`` (follows the 302 to a short-lived URL)."""
        return self._client._download(self.href, path, chunk_size=chunk_size)

    def __repr__(self):
        return f"<Asset {self.key} {self.type or ''}>"


class StacItem:
    """A STAC item with convenient asset access."""

    def __init__(self, client, data):
        self._client = client
        self._raw = data
        self.id = data.get("id")
        self.collection = data.get("collection")
        self.geometry = data.get("geometry")
        self.bbox = data.get("bbox")
        self.properties = data.get("properties", {})
        self.assets = {k: Asset(client, k, v) for k, v in data.get("assets", {}).items()}

    def asset(self, key):
        """Return the named :class:`Asset`, or ``None``."""
        return self.assets.get(key)

    def __repr__(self):
        return f"<StacItem {self.id} ({self.collection})>"


class StacCatalog:
    """Walk the project's STAC 1.1 catalog (the assets lane)."""

    def __init__(self, client):
        self._client = client

    def landing(self):
        """The STAC landing Catalog (dict)."""
        return self._client._get("/stac/")

    def collections(self):
        """List collection dicts."""
        return self._client._get("/stac/collections/").get("collections", [])

    def collection(self, collection_id):
        return self._client._get(f"/stac/collections/{collection_id}/")

    def items(self, collection_id, **filters):
        """Iterate :class:`StacItem` for a collection (paged)."""
        path = f"/stac/collections/{collection_id}/items/"
        params = dict(filters)
        params.setdefault("limit", 100)
        url = self._client._url(path)
        first = True
        while url:
            fc = self._client._get_url(url, params=params if first else None)
            first = False
            for feat in fc.get("features", []):
                yield StacItem(self._client, feat)
            nxt = [l["href"] for l in fc.get("links", []) if l.get("rel") == "next"]
            url = nxt[0] if nxt else None

    def item(self, collection_id, item_id):
        data = self._client._get(
            f"/stac/collections/{collection_id}/items/{item_id}/")
        return StacItem(self._client, data)


class ExportJob:
    """A bulk export job — poll to completion, then download the artifact."""

    def __init__(self, client, create_response):
        self._client = client
        self.id = create_response.get("id")
        self.model = create_response.get("model")
        self.format = create_response.get("format")
        self.state = create_response.get("state", "queued")
        self._status_url = create_response.get("status_url")

    def status(self):
        """Current status dict (``state`` plus progress / download_url)."""
        data = self._client._get(f"/exports/{self.id}/", raw_status=True)
        self.state = data.get("state", self.state)
        return data

    def wait(self, poll_seconds=2.0, timeout=900):
        """Block until the job is ``done`` (returns self) or raise on error/timeout."""
        waited = 0.0
        while waited <= timeout:
            data = self.status()
            state = data.get("state")
            if state == "done":
                self.state = "done"
                if data.get("empty"):
                    raise ExportError("Export contained no data.")
                return self
            if state == "error":
                raise ExportError(data.get("error_message", "Export failed."))
            time.sleep(poll_seconds)
            waited += poll_seconds
        raise ExportError(f"Export {self.id} did not finish within {timeout}s.")

    def download(self, path, chunk_size=1 << 20):
        """Download the finished artifact (follows the short-SAS 302)."""
        url = self._client._url(f"/exports/{self.id}/download/")
        return self._client._download(url, path, chunk_size=chunk_size)


class Client:
    """A grant-scoped geoDB protocol client.

    Args:
        token: the access grant (``gdbg_…``) or a Knox token.
        base_url: the geoDB base URL (default ``https://api.geodb.io``).
        auth_scheme: ``"Grant"`` (default) or ``"Token"`` for Knox first-party use.
        api_prefix: path prefix for the v2 API (default ``/api/v2``; use ``/v2``
            behind the api.* subdomain rewrite).
        timeout: per-request timeout (seconds).
    """

    def __init__(self, token, base_url=_DEFAULT_BASE, *, auth_scheme="Grant",
                 api_prefix=_DEFAULT_PREFIX, timeout=_DEFAULT_TIMEOUT, session=None):
        if not token:
            raise ValueError("token is required")
        self.base_url = base_url.rstrip("/")
        self.api_prefix = "/" + api_prefix.strip("/")
        self.timeout = timeout
        self._auth_header = f"{auth_scheme} {token}"
        self._session = session or requests.Session()

    # ── HTTP core ──────────────────────────────────────────────────────────
    def _url(self, path):
        return f"{self.base_url}{self.api_prefix}/{path.lstrip('/')}"

    def _headers(self):
        return {"Authorization": self._auth_header, "Accept": "application/json"}

    def _get(self, path, params=None, raw_status=False):
        return self._get_url(self._url(path), params=params, raw_status=raw_status)

    def _get_url(self, url, params=None, raw_status=False):
        resp = self._session.get(url, params=params, headers=self._headers(),
                                 timeout=self.timeout)
        return self._handle(resp, raw_status=raw_status)

    def _post(self, path, json=None):
        resp = self._session.post(self._url(path), json=json,
                                  headers=self._headers(), timeout=self.timeout)
        return self._handle(resp)

    @staticmethod
    def _body(resp):
        try:
            return resp.json()
        except ValueError:
            return None

    def _handle(self, resp, raw_status=False):
        if resp.status_code in (401, 403):
            body = self._body(resp)
            code = body.get("reason_code") if isinstance(body, dict) else None
            raise AuthError(f"Access denied ({resp.status_code}"
                            + (f", {code}" if code else "")
                            + "). Check the grant token and its project scope.")._keep(body)
        if resp.status_code == 404:
            raise NotFoundError(f"Not found: {resp.url}")._keep(self._body(resp))
        # Export status uses 202 (running) / 200 (done) / 500 (error) as signal.
        if raw_status and resp.status_code in (200, 202, 500):
            try:
                return resp.json()
            except ValueError:
                return {}
        if not resp.ok:
            raise APIError(resp.status_code, resp.text[:300], resp.url)
        if resp.content:
            return resp.json()
        return {}

    def _download(self, url, path, chunk_size=1 << 20):
        # The first hop is the authenticated asset/export redirect endpoint. Do
        # NOT auto-follow: the 302 Location is a self-authenticating URL (a
        # short-lived signed blob URL in prod, or a dev media URL) and the grant
        # token must never travel to it — the read-only grant gate rejects the
        # grant header on any non-API path (a same-host dev redirect would 403).
        # So resolve the redirect ourselves and fetch the target UNauthenticated.
        resp = self._session.get(url, headers=self._headers(),
                                 allow_redirects=False, timeout=self.timeout)
        if resp.status_code in (301, 302, 303, 307, 308):
            target = resp.headers.get("Location")
            if not target:
                raise APIError(resp.status_code, "redirect without Location", url)
            from urllib.parse import urljoin
            resp = self._session.get(urljoin(url, target), stream=True,
                                     timeout=self.timeout)
        if resp.status_code in (401, 403):
            raise AuthError("Access denied downloading asset.")
        if resp.status_code == 404:
            raise NotFoundError(f"Asset not found: {url}")
        resp.raise_for_status()
        with resp:
            with open(path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    if chunk:
                        fh.write(chunk)
        return path

    # ── Records lane ───────────────────────────────────────────────────────
    def project(self):
        """The grant/project context for this client (dict)."""
        return self._get("/grant-context/")

    def collars(self, **filters):
        return Paginated(self, "/drill-collars/", filters)

    def samples(self, **filters):
        return Paginated(self, "/drill-samples/", filters)

    def assays(self, **filters):
        return Paginated(self, "/assays/", filters)

    def point_samples(self, **filters):
        return Paginated(self, "/point-samples/", filters)

    def qc_samples(self, **filters):
        """QA/QC samples (standards, blanks, duplicates)."""
        return Paginated(self, "/qc-samples/", filters)

    def surveys(self, **filters):
        """Geophysical surveys (metadata + WGS84 footprint).

        NOTE: geophysical, not downhole. Downhole survey stations (azimuth/dip
        down the hole) are :meth:`drill_surveys`.
        """
        return Paginated(self, "/geophysical-surveys/", filters)

    # ── The downhole interval tables ───────────────────────────────────────
    # The same tables geoDB serves to Leapfrog and Vulcan over ODBC. Each is a
    # depth interval (or a depth station) hung off a collar's `bhid`.

    def drill_surveys(self, **filters):
        """Downhole survey stations — depth_at, azimuth, dip."""
        return Paginated(self, "/drill-surveys/", filters)

    def lithology(self, **filters):
        """Downhole lithology intervals."""
        return Paginated(self, "/drill-lithologies/", filters)

    def alteration(self, **filters):
        """Downhole alteration intervals."""
        return Paginated(self, "/drill-alterations/", filters)

    def structures(self, **filters):
        """Downhole structural measurements — point (depth_at only) and zone
        (depth_from/depth_to) in one table; filter on the depth fields to split
        them the way the exports do."""
        return Paginated(self, "/drill-structures/", filters)

    def mineralization(self, **filters):
        """Downhole mineralization intervals (mineral percentages)."""
        return Paginated(self, "/drill-mineralizations/", filters)

    def veins(self, **filters):
        """Downhole vein intervals (type, width, mineral contents)."""
        return Paginated(self, "/drill-veins/", filters)

    def rqd(self, **filters):
        """Downhole geotech: core recovery + rock mass (RQD, Q-system, RMR)."""
        return Paginated(self, "/drill-rqds/", filters)

    def spectral(self, **filters):
        """Downhole spectral intervals (geounit abundances)."""
        return Paginated(self, "/drill-spectral-intervals/", filters)

    def custom_intervals(self, **filters):
        """User-defined downhole intervals. TYPE IS DATA: a categorical row
        carries its value's name, a numeric row carries its measure."""
        return Paginated(self, "/drill-custom-intervals/", filters)

    # ── Writing (the ONE write endpoint) ───────────────────────────────────
    # Every write is POST /api/v2/records/ with {model, intent, records}; a
    # key writes only if project() says read_only is false. Answers are per
    # ROW (WriteResult.rows / .refused); only a refusal of the whole request
    # raises WriteRefused. Each write returns a write_id that undo() reverses.

    def _records(self, body, idempotency_key=None):
        headers = self._headers()
        if idempotency_key:
            headers["Idempotency-Key"] = str(idempotency_key)
        resp = self._session.post(self._url("/records/"), json=body, headers=headers,
                                  timeout=self.timeout)
        if resp.status_code == 401:
            self._handle(resp)
        if resp.status_code != 200:
            raise WriteRefused(resp.status_code, self._body(resp), resp.url)
        return WriteResult(self, self._body(resp) or {})

    def describe(self, model, project=None):
        """The live write contract for ``model``: its fields (and choices), the
        identifying fields, the set its rows belong to (with the project's
        sets and which this key may write), the coordinate rule, and which
        intents need the user's go-ahead first."""
        params = {"project": project} if project is not None else None
        return self._get(f"/records/describe/{model}/", params=params)

    def write(self, model, rows, intent="create", *, logging_set=None, project=None,
              idempotency_key=None, dry_run=False, acknowledge=None, confirm=None):
        """Write ``rows`` (a list of dicts or a DataFrame) through the one gate.

        Args:
            model: e.g. ``"DrillCollar"``, ``"DrillSample"``, ``"DrillLithology"``
                (``describe`` / ``project()["writes"]["models"]`` list them).
            intent: ``"create"`` (never overwrites; an existing record with other
                values is skipped and named), ``"upsert"``, or ``"update"``
                (existing records only, by identity or geoDB ``id``). Upsert and
                update change the user's data: dry-run first and ask the user.
            logging_set: the SET the rows belong to — sent as the body's
                ``"set"``, for every set-aware family (lithology, alteration, …
                and drill-sample sets alike): an existing set's name, or
                ``{"name": "…", "create": True}`` for a new one. Required for
                set-aware models; never guess it, ask the user.
            idempotency_key: send one (e.g. a uuid you keep) to make a retry of
                the same request safe: it replays the first answer.
            dry_run: True validates — the same per-row outcomes, nothing written.
            acknowledge: e.g. ``["create_catalog_entries"]`` after asking the user.

        Coordinates carry their own ``epsg``, in the numbers you have; never
        pre-convert. Returns a :class:`WriteResult`.

        ``make_default_set`` has no method of its own on purpose: it changes
        what everyone on the project sees, runs only through a PERSON's own key
        (a vendor key is always refused) and only on their explicit request.
        Send it through this call — ``write(model, [],
        intent="make_default_set", logging_set="<set>", dry_run=True)``, then
        again with ``confirm=<the dry run's "confirm">`` after the user's yes.
        """
        body = {"model": model, "intent": intent, "records": rows_from(rows),
                "dry_run": bool(dry_run)}
        if logging_set is not None:
            body["set"] = logging_set
        if project is not None:
            body["project"] = project
        if acknowledge:
            body["acknowledge"] = list(acknowledge)
        if confirm is not None:
            body["confirm"] = confirm
        return self._records(body, idempotency_key)

    def validate(self, model, rows, intent="create", **kwargs):
        """``write(..., dry_run=True)``: every row's outcome, nothing written."""
        kwargs["dry_run"] = True
        return self.write(model, rows, intent, **kwargs)

    def retract(self, model, rows, *, confirm=False, dry_run=False, project=None,
                idempotency_key=None):
        """Move records — and everything that belongs to them — to the Trash.

        Run it with ``dry_run=True`` first and show the user what it lists
        (``result["cascade"]``); send it with ``confirm=True`` only after their
        yes. Without the confirm the server refuses (``confirm_required``).
        Each row names a record by its identifying fields or its geoDB ``id``.
        Undo (or :meth:`restore`) brings everything back; nothing is ever
        purged through the API."""
        return self.write(model, rows, "retract", dry_run=dry_run, project=project,
                          idempotency_key=idempotency_key,
                          confirm="retract" if confirm else None)

    def restore(self, write_id=None, *, audit_batch_id=None, dry_run=False,
                idempotency_key=None):
        """Bring a removed batch back from the Trash: a retract's ``write_id``,
        or any Trash batch's ``audit_batch_id``. A restore is a write of its own."""
        if (write_id is None) == (audit_batch_id is None):
            raise ValueError("name exactly one of write_id or audit_batch_id")
        body = {"intent": "restore", "dry_run": bool(dry_run)}
        if write_id is not None:
            body["write_id"] = str(write_id)
        else:
            body["audit_batch_id"] = str(audit_batch_id)
        return self._records(body, idempotency_key)

    def undo(self, write_id, dry_run=False, *, idempotency_key=None):
        """Reverse one earlier write by its ``write_id``. Rows changed by a later
        write are left as they are and named (``undo_stale``); the result's
        ``complete`` says whether every row was reversed. A retry with the same
        ``idempotency_key`` replays the first answer."""
        return self._records({"intent": "undo", "write_id": str(write_id),
                              "dry_run": bool(dry_run)}, idempotency_key)

    def writes(self, write_id=None, **filters):
        """What this key wrote, newest first (each with its undo handle), as a
        paginated iterator — or one write (with its rows) when ``write_id`` is
        given. Filters: ``model``, ``intent``, ``project``, ``undone``."""
        if write_id is not None:
            return self._get(f"/records/writes/{write_id}/")
        if "undone" in filters and isinstance(filters["undone"], bool):
            filters["undone"] = "true" if filters["undone"] else "false"
        return Paginated(self, "/records/writes/", filters, page_size=50)

    # ── Assets lane ────────────────────────────────────────────────────────
    def stac(self):
        """The project's STAC 1.1 catalog walker."""
        return StacCatalog(self)

    # ── Bulk lane ──────────────────────────────────────────────────────────
    def export(self, model, format="geoparquet", include_assays=True):
        """Create a bulk export job. Returns an :class:`ExportJob` (call .wait())."""
        resp = self._post("/exports/", json={
            "model": model, "format": format, "include_assays": include_assays})
        return ExportJob(self, resp)
