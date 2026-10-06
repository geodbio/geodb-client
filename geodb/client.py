"""
The geoDB Open Exploration Protocol Python client.

    import geodb
    gx = geodb.Client(token="gdbg_...", base_url="https://api.geodb.io")
    df = gx.collars().to_dataframe()

A ``Client`` authenticates with an access grant (``Authorization: Grant
<token>``) scoped to the project(s) it was given. geoDB keeps no current
project between calls: when the key reads several, name one on every read —
``gx.collars(project=12)`` — or the server answers ``ProjectRequired`` with the
choices; ``company=`` names a company (the company-level tables), and
``scope="company", company=…`` reads every project of one company. An interval
table over a project with several sets answers ``SetChoiceRequired`` until you
name one (``set=<id or name>``) or ask for all (``set="all"``). Knox tokens work
too (``auth_scheme="Token"``) for first-party use.
"""

import time

import requests

from .errors import (AuthError, NotFoundError, APIError, ExportError, ProtocolVersionMismatch,
                     WriteRefused, error_for)
from .writes import WriteResult, rows_from

__all__ = ["Client", "PROTOCOL_VERSION", "client_requirement"]

#: The protocol version this client was built for (geoDB's
#: ``api/protocol_version.py``). Sent on every request and compared with the
#: server's ``X-GeoDB-Protocol-Version``: a different major.minor raises
#: :class:`ProtocolVersionMismatch` naming the install line.
PROTOCOL_VERSION = "0.3.1"
VERSION_HEADER = "X-GeoDB-Protocol-Version"


def _minor(version):
    try:
        major, minor = (int(p) for p in str(version).split(".")[:2])
        return major, minor
    except (TypeError, ValueError):
        return None


def client_requirement(version):
    """The geodb-client releases that speak protocol ``version``."""
    major, minor = _minor(version)
    return f"geodb-client>={major}.{minor},<{major}.{minor + 1}"

_DEFAULT_TIMEOUT = 60
_DEFAULT_BASE = "https://api.geodb.io"
_DEFAULT_PREFIX = "/api/v2"


class Paginated:
    """A lazy iterator over a v2 LimitOffset endpoint.

    Iterate for records; call :meth:`to_dataframe` to pull everything into a
    pandas DataFrame. Pages come in ORDER. Once the first page says the total
    (``count``), the rest are fetched ``parallel`` at a time (default 4) by
    offset; ``parallel=1`` — or ``skip_count=True``, which asks the server not
    to count — follows ``next`` one page at a time.
    """

    def __init__(self, client, path, params=None, page_size=500, *, parallel=None):
        self._client = client
        self._path = path
        self._params = dict(params or {})
        self._params.setdefault("limit", page_size)
        if self._params.get("skip_count") in (True, "1", "true", "True", 1):
            self._params["skip_count"] = "1"
        elif self._params.get("skip_count") in (False, None, "0", "false", 0):
            self._params.pop("skip_count", None)
        self._parallel = max(1, int(parallel if parallel is not None
                                    else getattr(client, "parallel", 4)))
        self._count = None
        #: The first page's envelope keys other than ``results`` (``withheld``,
        #: ``limit_clamped``, …) — what the server said about the whole read.
        self.envelope = {}

    # -- page links -------------------------------------------------------
    def _link(self, next_link, **changes):
        """A page link built on the CONFIGURED base URL (the key only ever
        goes there), carrying the query of the server's ``next`` link —
        every parameter the read sent, repeated ones kept — with ``changes``
        applied."""
        from urllib.parse import parse_qsl, urlencode, urlsplit
        pairs = [(k, v) for k, v in parse_qsl(urlsplit(next_link).query,
                                              keep_blank_values=True)
                 if k not in changes]
        pairs += [(k, str(v)) for k, v in changes.items()]
        return self._client._url(self._path) + "?" + urlencode(pairs, doseq=True)

    @staticmethod
    def _step(next_link):
        """``(offset, page size)`` the server's ``next`` link names — the
        stride is the server's page size, never the first page's length (a
        page may come back short)."""
        from urllib.parse import parse_qs, urlsplit
        query = parse_qs(urlsplit(next_link).query)
        try:
            return int(query["offset"][-1]), int(query["limit"][-1])
        except (KeyError, ValueError, IndexError):
            return None

    def _pages_by_offset(self, data):
        """The pages after the first when its total is known, fetched
        ``parallel`` at a time and yielded IN ORDER (each page's whole
        envelope); None when they cannot be (no count, no next page,
        skip_count, parallel=1, a next link without offset/limit). A page that
        fails raises its error where it falls — the pages before it are
        yielded, nothing after it, never a silent gap."""
        step = self._step(data["next"]) if data.get("next") else None
        if (self._parallel < 2 or step is None or self._count is None
                or "skip_count" in self._params):
            return None
        first_offset, size = step
        if size < 1:
            return None
        offsets = range(first_offset, self._count, size)
        if not offsets:
            return None
        from concurrent.futures import ThreadPoolExecutor
        next_link = data["next"]

        def page(offset):
            return self._client._get_url(self._link(next_link, limit=size, offset=offset))

        def pages():
            with ThreadPoolExecutor(max_workers=self._parallel) as pool:
                for result in pool.map(page, offsets):
                    yield result
        return pages()

    def __iter__(self):
        params = dict(self._params)
        url = self._client._url(self._path)
        data = self._client._get_url(url, params=params)
        if not isinstance(data, dict) or "results" not in data:
            # non-paginated payload (shouldn't happen on v2 list endpoints)
            for row in (data if isinstance(data, list) else [data]):
                yield row
            return
        if data.get("count") is not None:
            self._count = data["count"]       # the envelope's total, kept
        self.envelope = {k: v for k, v in data.items() if k != "results"}
        for row in data["results"]:
            yield row
        rest = self._pages_by_offset(data)
        if rest is not None:
            for page in rest:
                for row in page.get("results") or []:
                    yield row
                data = page
        # Follow ``next`` from wherever the pages stopped: the sequential read,
        # or rows added past the first page's count during a parallel one.
        while data.get("next"):
            data = self._client._get_url(self._link(data["next"]))
            for row in data.get("results") or []:
                yield row

    def to_dataframe(self):
        """Materialise all records into a :class:`pandas.DataFrame`."""
        import pandas as pd
        return pd.DataFrame(list(self))

    def count(self):
        """Total matching records: the list envelope's ``count`` — no extra
        round trip once the rows were read; otherwise one ``limit=1`` page."""
        if self._count is None:
            params = {k: v for k, v in self._params.items() if k != "skip_count"}
            data = self._client._get(self._path, params={**params, "limit": 1})
            self._count = data.get("count") if isinstance(data, dict) else None
        return self._count


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
        #: The guide sections to read before reporting from this table
        #: (``[{topic, section, url, why}]``; fetch one with
        #: ``client.guide(topic, section)``) and what the file merged — both
        #: from the job's status.
        self.see_guide = []
        self.notes = {}

    def status(self):
        """Current status dict (``state`` plus progress / download_url)."""
        data = self._client._get(f"/exports/{self.id}/", raw_status=True)
        self.state = data.get("state", self.state)
        self.see_guide = data.get("see_guide") or self.see_guide
        self.notes = data.get("notes") or self.notes
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
        parallel: pages fetched at once when a list's total is known
            (default 4; 1 reads one page at a time).
    """

    def __init__(self, token, base_url=_DEFAULT_BASE, *, auth_scheme="Grant",
                 api_prefix=_DEFAULT_PREFIX, timeout=_DEFAULT_TIMEOUT, session=None,
                 check_protocol=True, parallel=4):
        if not token:
            raise ValueError("token is required")
        #: Compare the server's protocol version with :data:`PROTOCOL_VERSION`
        #: (once, on the first response that carries it).
        self.check_protocol = check_protocol
        self.parallel = max(1, int(parallel))
        self.server_protocol_version = None
        self.base_url = base_url.rstrip("/")
        self.api_prefix = "/" + api_prefix.strip("/")
        self.timeout = timeout
        self._auth_header = f"{auth_scheme} {token}"
        self._session = session or requests.Session()

    # ── HTTP core ──────────────────────────────────────────────────────────
    def _url(self, path):
        return f"{self.base_url}{self.api_prefix}/{path.lstrip('/')}"

    def _headers(self):
        return {"Authorization": self._auth_header, "Accept": "application/json",
                VERSION_HEADER: PROTOCOL_VERSION}

    def _check_protocol(self, resp):
        """Raise :class:`ProtocolVersionMismatch` when the server's protocol
        major.minor differs from the one this client speaks. A response
        without the header (an older server, a proxy page) is not judged."""
        if not self.check_protocol:
            return
        server = self.server_protocol_version
        if server is None:
            headers = getattr(resp, "headers", None) or {}
            server = headers.get(VERSION_HEADER)
            if not server:
                return
            self.server_protocol_version = server
        # Raised on EVERY call once known (review #2 nit), never just the first.
        if _minor(server) is not None and _minor(server) != _minor(PROTOCOL_VERSION):
            raise ProtocolVersionMismatch(server, PROTOCOL_VERSION,
                                          f'pip install "{client_requirement(server)}"')

    def _get(self, path, params=None, raw_status=False):
        return self._get_url(self._url(path), params=params, raw_status=raw_status)

    def _get_url(self, url, params=None, raw_status=False):
        resp = self._session.get(url, params=params, headers=self._headers(),
                                 timeout=self.timeout)
        self._check_protocol(resp)
        return self._handle(resp, raw_status=raw_status)

    def _post(self, path, json=None):
        resp = self._session.post(self._url(path), json=json,
                                  headers=self._headers(), timeout=self.timeout)
        self._check_protocol(resp)
        return self._handle(resp)

    @staticmethod
    def _body(resp):
        try:
            return resp.json()
        except ValueError:
            return None

    def _handle(self, resp, raw_status=False):
        # Export status uses 202 (running) / 200 (done) / 500 (error) as signal.
        if raw_status and resp.status_code in (200, 202, 500):
            try:
                return resp.json()
            except ValueError:
                return {}
        if not resp.ok:
            # One exception class per kind of refusal, each carrying the
            # server's reason_code + remedy (errors.error_for).
            raise error_for(resp.status_code, self._body(resp), resp.url,
                            text=resp.text[:300], headers=resp.headers)
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
        """The credential's context (dict): its companies and projects (each
        with its per-family set counts), whether it may write, its limits."""
        return self._get("/grant-context/")

    grant_context = project

    def assay_results(self, project=None, *, company=None, scope=None, **filters):
        """The flat assay table — one row per sample × element × method, with
        the method, certificate and laboratory by id (join ``methods/``):
        ``gx.assay_results(project=12).to_dataframe()``. Takes the assay
        list's filters; pages of 2,000, fetched in parallel. For a WHOLE
        project, ``gx.export("assay_results", project=12)`` is faster: one
        file, every flag kept, with the method / certificate / laboratory
        names. ``element="Au"`` filters to one element. Assays whose sample
        is in the Trash are withheld (the envelope's ``withheld``);
        ``include_trashed_samples="true"`` returns them, marked. A row's
        ``data_warnings`` flag a value outside its method's limits."""
        return Paginated(self, "/assay-results/",
                         read_params(filters, project=project, company=company, scope=scope),
                         page_size=2000)

    def methods(self, company=None, *, project=None, **filters):
        """The laboratory methods (a company-level table: name the company, or
        a project to read its company's)."""
        return Paginated(self, "/methods/",
                         read_params(filters, project=project, company=company))

    def laboratories(self, company=None, *, project=None, **filters):
        """The laboratories (a company-level table)."""
        return Paginated(self, "/laboratories/",
                         read_params(filters, project=project, company=company))

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
        self._check_protocol(resp)
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
              idempotency_key=None, dry_run=False, acknowledge=None, confirm=None,
              layer=None):
        """Write ``rows`` (a list of dicts or a DataFrame) through the one gate.

        Every change to the user's geoDB is this ONE call (``POST /api/v2/
        records/``), answered per row. Read the live contract first
        (:meth:`describe`), dry-run (``dry_run=True`` or :meth:`validate`), then
        send; every write that changed something returns a ``write_id`` that
        :meth:`undo` reverses.

        Args:
            model: the record type, e.g. ``"DrillCollar"``, ``"DrillSample"``,
                ``"DrillLithology"``, ``"Assay"`` (``describe`` /
                ``project()["writes"]["models"]`` list them), or one of the
                faces below.
            intent: what the write does (the intents below). Anything but
                ``"create"`` and ``"undo"`` changes what exists: dry-run first,
                tell the user exactly what will change, and send it only after
                their yes.
            logging_set: the SET the rows belong to — sent as the body's
                ``"set"``, for every set-aware family (lithology, alteration, …
                and drill-sample sets alike): an existing set's name, or
                ``{"name": "…", "create": True}`` for a new one. Required for
                set-aware models; never guess it, ask the user.
            project: the project the rows belong to (geoDB keeps no current
                project; name it).
            idempotency_key: send one (e.g. a uuid you keep) to make a retry of
                the same request safe: it replays the first answer.
            dry_run: True validates — the same per-row outcomes, nothing written.
            acknowledge: consents the user gave, e.g.
                ``["create_catalog_entries"]``, ``["replace_values"]``,
                ``["crs_implausible"]`` (``describe`` lists them; ask first).
            confirm: the value a dry run returned for an intent that needs one
                (``"retract"`` for a retract; ``make_default_set``,
                ``make_export_set``, ``qaqc_verdict``, ``publish`` and every
                ``Project`` intent return their own).
            layer: ``VectorLayer`` only — the layer the rows (its features)
                land in: ``{"name": …, "kind": …}`` (optional
                ``display_name``, ``attribution``, ``style``, ``activate``;
                ``describe("VectorLayer")["layer"]`` lists the kinds). Each row
                is one feature: ``geometry`` (WKT) in its ``epsg``, other keys
                kept as its attributes. One write lands one new layer (a draft
                unless ``activate`` — ask first); the answer's ``layer`` gives
                its id and where the draft can be read; Undo removes it with
                its features.

        The intents (protocol 0.3, the v0.4 surface):

        * ``"create"`` — adds records; an existing record with other values is
          skipped and every differing field named (``conflicts``); never
          overwrites.
        * ``"upsert"`` — adds new records and overwrites exactly the fields
          sent on existing ones. On a long-form record (an ``Assay``'s
          results, a method's limits, a standard's certified values) it ADDS a
          value the record lacks; overwriting a stored result needs
          ``acknowledge=["replace_values"]``. Send a result as the lab wrote
          it: a below-detection ``"<0.005"``, ``"BDL"``, ``"ND"`` or a negative
          is stored as the sentinel -1 (a write never stores a limit on the
          method: the method's own limit, a floating method's per-sample
          limit, or none, as the write says); never
          send 0 or half the limit. ``"N.D."`` / ``"N/D"`` is refused
          ``ambiguous_nd``: ask the user (not detected → ``"BDL"``; not
          determined → leave the value out).
        * ``"update"`` — changes the fields sent on EXISTING records (by
          identity or geoDB ``id``); never creates. ``"field": None`` empties a
          field. A unit correction on an ``Assay`` is ONE update naming the
          ``certificate``, the ``element`` and the true ``unit``.
        * ``"retract"`` — to the Trash with everything that belongs to them;
          use :meth:`retract` (it sends ``confirm="retract"``).
        * ``"restore"`` — a removed batch back; use :meth:`restore`.
        * ``"make_default_set"`` / ``"make_export_set"`` — make a set what
          everyone sees / what exports and ODBC read; a person's own key only.
        * ``"qaqc_verdict"`` (model ``"Certificate"``) — approve, reject or
          link a re-assay, ONLY on the user's explicit request.
        * ``"qc_reconnect"`` (model ``"QCSample"``) — reconnect a QC sample
          to its withdrawn result.
        * ``"undo"`` — reverse one write by its ``write_id``; use :meth:`undo`.

        The faces (the same call; ``describe(model)`` is each one's contract):

        * ``Project`` (one per request): ``"create"`` (needs ``company``: ask
          the user which) · ``"update"`` · ``"set_coordinate_system"`` ·
          ``"set_state"`` · ``"retract"`` · ``"restore"``; each real request
          sends the ``confirm`` its dry run returned.
        * ``Report`` (``"create"`` · ``"update"`` · ``"publish"``, an informal
          report only and only when asked), ``ReportSection`` (``"create"`` ·
          ``"update"`` with the ``expected_revision`` you read ·
          ``"retract"``) and ``ReportFigure`` (``"create"``).
        * Settings — ``CustomFieldSchema`` (custom columns),
          ``ColumnConfiguration``, ``AssayMergeSettings``,
          ``AssayRangeConfiguration`` and ``MetalEquivalentConfig`` (price
          decks): ``"create"`` · ``"update"`` (children edited through the
          parent, each ``{"action": "add"|"change"|"remove", …}``) ·
          ``"retract"`` · ``"restore"``; ``QAQCProtocol``: ``"create"`` ·
          ``"update"``; ``QCConfiguration`` and ``ODBCSettings`` (the
          project's own, one each): ``"update"``. A person's own key only,
          with their permission for the area; each answers its web page as
          ``url``. A change that moves QC verdicts answers its dry run with
          ``verdicts`` and a ``confirm``: send that ``confirm=`` after the
          user's yes.
        * ``VectorLayer`` — ``"create"`` with ``layer=`` (above).

        Coordinates carry their own ``epsg``, in the numbers you have; never
        pre-convert. Returns a :class:`WriteResult`.

        ``make_default_set``, ``make_export_set`` and ``qaqc_verdict`` have no
        methods of their own on purpose: each changes what everyone on the
        project sees, runs only through a PERSON's own key (a vendor key is
        always refused) and only on their explicit request. Send them through
        this call with a dry run first, then again with
        ``confirm=<the dry run's "confirm">`` after the user's yes — e.g.
        ``write(model, [], intent="make_default_set", logging_set="<set>",
        dry_run=True)``, or ``write("Certificate", [{...}],
        intent="qaqc_verdict", dry_run=True)``. ``qc_reconnect`` (model
        ``"QCSample"``) also goes through this call: dry-run it and ask first,
        as for any change to existing records; it needs no confirm value.
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
        if layer is not None:
            body["layer"] = layer
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

    # ── The domain guide ───────────────────────────────────────────────────
    def guide(self, topic=None, section=None):
        """geoDB's domain guide: the topic index, a whole topic, or one
        section (``gx.guide("assay-values", "below-detection")``) — what a
        read's ``see_guide`` points at."""
        if topic is None:
            return self._get("/guide/")
        return self._get(f"/guide/{topic}/" + (f"{section}/" if section else ""))

    # ── Assets lane ────────────────────────────────────────────────────────
    def stac(self):
        """The project's STAC 1.1 catalog walker."""
        return StacCatalog(self)

    # ── Bulk lane ──────────────────────────────────────────────────────────
    def export(self, model, format="geoparquet", include_assays=True, *, project=None,
               company=None, scope=None, set=None, merge_settings_id=None,
               include_trashed_samples=None):
        """Create a bulk export job of ONE project's table. Returns an
        :class:`ExportJob` (call .wait()). The fastest way to a whole table:
        ``gx.export("assay_results", project=12).wait().download("a.parquet")``.
        ``model`` names the table — ``"assay_results"`` is every assay value
        (one row per sample × element × method, below-detection / over-range
        / withheld flags and detection limits kept: the table for
        statistics); ``"drill_samples"`` has the values MERGED per the
        project's settings (one value per element, flags not kept — or one
        column per method when the settings' ``merge_mode`` says so), or per
        ``merge_settings_id`` (``drill_samples``, ``point_samples``,
        ``qc_samples``) — the job's answer states the settings applied
        (``merge_settings``) and names any method / element pairs whose
        below-detection results have no detection limit. ``project``
        is needed when the key reads several projects; ``set`` (id, name or
        ``"all"``) when the table's project holds several sets of it. A
        parameter the export cannot honour is refused, never answered with an
        empty file."""
        body = {"model": model, "format": format, "include_assays": include_assays}
        body.update(read_params({}, project=project, company=company, scope=scope, set=set))
        if merge_settings_id is not None:
            body["merge_settings_id"] = merge_settings_id
        if include_trashed_samples is not None:
            # assay_results: assays whose sample is in the Trash are left out
            # by default; True includes them, marked.
            body["include_trashed_samples"] = bool(include_trashed_samples)
        resp = self._post("/exports/", json=body)
        return ExportJob(self, resp)


def read_params(filters, *, project=None, company=None, scope=None, set=None):
    """A read's query: the caller's filters plus the scope question (only
    what was named — the server keeps no current project)."""
    params = dict(filters)
    for key, value in (("project", project), ("company", company),
                       ("scope", scope), ("set", set)):
        if value is not None:
            params[key] = value
    return params


#: THE table registry: (method, path, export models, sets?, what it is). The
#: list methods, the README's table and the contract test all read it.
TABLES = (
    ("collars", "/drill-collars/", ("drill_collars",), False,
     "Collar location, orientation, total depth"),
    ("drill_surveys", "/drill-surveys/", ("drill_surveys",), False,
     "Downhole survey stations (depth, azimuth, dip)"),
    ("lithology", "/drill-lithologies/", ("drill_lithology",), True,
     "Downhole lithology intervals"),
    ("alteration", "/drill-alterations/", ("drill_alteration",), True,
     "Downhole alteration intervals"),
    ("samples", "/drill-samples/", ("drill_samples",), True,
     "Drill samples (the assay by id; expand=\"assay\" for the record)"),
    ("structures", "/drill-structures/", ("drill_structure_point", "drill_structure_zone"),
     False, "Structural measurements; point (a depth) vs zone (an interval)"),
    ("mineralization", "/drill-mineralizations/", ("drill_mineralization",), True,
     "Mineralization intervals + mineral percentages"),
    ("veins", "/drill-veins/", ("drill_veins",), True,
     "Vein intervals (type, width, mineral contents)"),
    ("rqd", "/drill-rqds/", ("drill_rqd",), True, "Geotech: core recovery + rock mass"),
    ("spectral", "/drill-spectral-intervals/", ("drill_spectral",), True,
     "Spectral intervals (geounit abundances)"),
    ("custom_intervals", "/drill-custom-intervals/", ("drill_custom_intervals",), True,
     "User-defined intervals (TYPE IS DATA)"),
    ("point_samples", "/point-samples/", ("point_samples",), False,
     "Surface / soil / rock-chip samples"),
    ("qc_samples", "/qc-samples/", ("qc_samples",), False,
     "QA/QC (standards, blanks, duplicates)"),
    ("assays", "/assays/", ("assay_results",), False,
     "Assay results (flat table: assay_results(); every value: export(\"assay_results\"))"),
    ("surveys", "/geophysical-surveys/", ("geophysical_surveys",), False,
     "GEOPHYSICAL surveys (metadata + WGS84 footprint) — downhole surveys are "
     "drill_surveys()"),
)


#: Lists the server pages at 2,000 rows for an access grant (protocol 0.3:
#: assay-results/ and the lean drill-samples/ rows); every other list at 500.
_PAGE_SIZE = {"/drill-samples/": 2000}


def _list_method(name, path, sets, what):
    page_size = _PAGE_SIZE.get(path, 500)
    if sets:
        def method(self, project=None, *, company=None, scope=None, set=None, **filters):
            return Paginated(self, path, read_params(
                filters, project=project, company=company, scope=scope, set=set),
                page_size=page_size)
        extra = (' When the project holds several sets of it, name one with '
                 '``set=<id or name>`` or read them all with ``set="all"`` (each row '
                 'says its ``set_id`` / ``set_name``).')
    else:
        def method(self, project=None, *, company=None, scope=None, **filters):
            return Paginated(self, path, read_params(
                filters, project=project, company=company, scope=scope),
                page_size=page_size)
        extra = ''
    method.__name__ = name
    method.__qualname__ = f"Client.{name}"
    method.__doc__ = (f"{what} — ``GET /api/v2{path}``. ``project`` names the "
                      f"project (needed when the key reads several); "
                      f"``scope=\"company\", company=…`` reads one company's."
                      + extra)
    return method


for _name, _path, _exports, _sets, _what in TABLES:
    setattr(Client, _name, _list_method(_name, _path, _sets, _what))
