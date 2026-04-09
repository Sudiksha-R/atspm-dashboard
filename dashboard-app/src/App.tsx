import { Fragment, useEffect, useMemo, useState } from "react";
import { CircleMarker, MapContainer, Marker, Polyline, Popup, TileLayer } from "react-leaflet";
import { divIcon, LatLngExpression } from "leaflet";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";

type MetricCard = {
  id: string;
  label: string;
  value: string;
  description: string;
};

type TrendPoint = {
  label: string;
  aogPct: number;
  avgWait: number;
  greenOccupancyPct: number;
  redOccupancyPct: number;
  splitFailures: number;
  maxOuts: number;
  gapOuts: number;
  forceOffs: number;
  cycleLength: number;
};

type PhaseRow = {
  phase: number;
  aogPct: number;
  avgWait: number;
  maxWait: number;
  greenOccupancyPct: number;
  redOccupancyPct: number;
  greenTime: number;
  totalActuations: number;
  splitFailures: number;
  maxOuts: number;
  gapOuts: number;
  forceOffs: number;
  skips: number;
};

type IntersectionPayload = {
  id: string;
  name: string;
  route: string;
  lat: number;
  lon: number;
  region: string;
  summary: {
    meanAog: number;
    meanDelay: number;
    meanGreenOccupancy: number;
    meanRedOccupancy: number;
    cycleLength: number;
    splitFailures: number;
    maxOutCount: number;
    gapOutCount: number;
    forceOffCount: number;
  };
  insights: string[];
  phases: PhaseRow[];
  trend: TrendPoint[];
};

type DashboardResponse = {
  meta: {
    dataSource: string;
    note: string;
    availableDates: string[];
    defaultDateFrom: string;
    defaultDateTo: string;
    availableDays: string[];
    timeOfDayPresets: Array<{
      id: string;
      label: string;
      start: string;
      end: string;
    }>;
    selectionPresets: Array<{
      id: string;
      label: string;
      intersectionIds: string[];
    }>;
    metricDefinitions: Record<string, string>;
  };
  filters: {
    dateFrom: string;
    dateTo: string;
    daysOfWeek: string[];
    hourFrom: string;
    hourTo: string;
    timeOfDayPreset: string;
    selectedIntersectionIds: string[];
    selectedPhase: string;
  };
  corridor: {
    id: string;
    name: string;
    region: string;
    summary: {
      meanAog: number;
      meanDelay: number;
      meanGreenOccupancy: number;
      meanRedOccupancy: number;
      cycleLength: number;
      splitFailures: number;
      maxOutCount: number;
      gapOutCount: number;
      forceOffCount: number;
    };
    overviewMetrics: MetricCard[];
    phaseOperationMetrics: MetricCard[];
    availablePhases: number[];
    selectedPhase: string;
    trend: TrendPoint[];
    intersections: IntersectionPayload[];
    allIntersections: IntersectionPayload[];
  };
};

type HistoricalHour = {
  hour: number;
  label: string;
  status: string;
  sourceRows: number;
  message: string;
  aogPct: number;
  avgWait: number;
  splitFailures: number;
  maxOuts: number;
  gapOuts: number;
  forceOffs: number;
};

type HistoricalRunResponse = {
  exists: boolean;
  title: string;
  message?: string;
  signalId?: number;
  runDate?: string;
  sourceCsv?: string;
  dbPath: string;
  outputDir: string;
  filteredCsvPath: string;
  filteredRows?: number;
  detectorConfigRows?: number;
  createdAt?: string;
  summary?: {
    okHours: number;
    emptyHours: number;
    failedHours: number;
    totalSourceRows: number;
  };
  hours: HistoricalHour[];
  tableCounts: Array<{ name: string; rows: number }>;
};

type FilterState = {
  dateFrom: string;
  dateTo: string;
  daysOfWeek: string[];
  hourFrom: string;
  hourTo: string;
  timeOfDayPreset: string;
  selectedPhase: string;
};

const markerIcon = divIcon({
  className: "intersection-pin",
  html: "<span></span>",
  iconSize: [18, 18],
  iconAnchor: [9, 9]
});

const hourOptions = Array.from({ length: 24 }, (_, hour) => `${String(hour).padStart(2, "0")}:00`);
const fallbackDays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
const fallbackTimeOfDayPresets = [
  { id: "am-peak", label: "Weekday AM Peak", start: "06:00", end: "09:00" },
  { id: "midday", label: "Midday", start: "10:00", end: "14:00" },
  { id: "pm-peak", label: "Weekday PM Peak", start: "15:00", end: "19:00" },
  { id: "weekend-daytime", label: "Weekend Daytime", start: "09:00", end: "20:00" },
  { id: "custom", label: "Custom Period", start: "06:00", end: "18:00" }
];

function UiIcon({
  kind
}: {
  kind: "selection" | "warning" | "signal" | "clock" | "book" | "traffic" | "chart" | "info";
}) {
  const common = {
    width: 18,
    height: 18,
    viewBox: "0 0 24 24",
    fill: "none",
    stroke: "currentColor",
    strokeWidth: 1.8,
    strokeLinecap: "round" as const,
    strokeLinejoin: "round" as const
  };

  if (kind === "selection") {
    return (
      <svg {...common}>
        <path d="M4 7h16" />
        <path d="M4 12h10" />
        <path d="M4 17h7" />
        <circle cx="18" cy="12" r="3" />
      </svg>
    );
  }
  if (kind === "warning") {
    return (
      <svg {...common}>
        <path d="M12 4 20 19H4L12 4Z" />
        <path d="M12 9v4" />
        <path d="M12 17h.01" />
      </svg>
    );
  }
  if (kind === "signal") {
    return (
      <svg {...common}>
        <path d="M5 17V7" />
        <path d="M5 7h11" />
        <circle cx="9" cy="17" r="2" />
        <circle cx="14" cy="12" r="2" />
        <circle cx="18" cy="7" r="2" />
      </svg>
    );
  }
  if (kind === "clock") {
    return (
      <svg {...common}>
        <circle cx="12" cy="12" r="8" />
        <path d="M12 8v5l3 2" />
      </svg>
    );
  }
  if (kind === "book") {
    return (
      <svg {...common}>
        <path d="M5 6.5A2.5 2.5 0 0 1 7.5 4H19v15H7.5A2.5 2.5 0 0 0 5 21Z" />
        <path d="M5 6.5V21" />
      </svg>
    );
  }
  if (kind === "traffic") {
    return (
      <svg {...common}>
        <rect x="9" y="3" width="6" height="14" rx="2" />
        <path d="M12 17v4" />
        <circle cx="12" cy="7" r="1" />
        <circle cx="12" cy="10.5" r="1" />
        <circle cx="12" cy="14" r="1" />
      </svg>
    );
  }
  if (kind === "chart") {
    return (
      <svg {...common}>
        <path d="M4 19h16" />
        <path d="M7 16V9" />
        <path d="M12 16V5" />
        <path d="M17 16v-7" />
      </svg>
    );
  }
  return (
    <svg {...common}>
      <circle cx="12" cy="12" r="8" />
      <path d="M12 10v5" />
      <path d="M12 7h.01" />
    </svg>
  );
}

function MetricInfo({ description }: { description: string }) {
  return (
    <span className="tooltip-anchor" tabIndex={0} title={description}>
      <span className="metric-info-icon">
        <UiIcon kind="info" />
      </span>
      <span className="tooltip-bubble">{description}</span>
    </span>
  );
}

function MetricCardView({ card }: { card: MetricCard }) {
  return (
    <article className="metric metric-v1">
      <div className="metric-topline">
        <span>{card.label}</span>
        <MetricInfo description={card.description} />
      </div>
      <strong>{card.value}</strong>
    </article>
  );
}

function HistoricalRunView({
  historical,
  loading,
  error
}: {
  historical: HistoricalRunResponse | null;
  loading: boolean;
  error: string | null;
}) {
  const hours = historical?.hours ?? [];
  const maxRows = Math.max(1, ...hours.map((hour) => hour.sourceRows));

  return (
    <main className="workspace historical-workspace">
      {error ? <div className="status-banner error">{error}</div> : null}
      {loading ? <div className="status-banner">Loading the hourly SQLite run summary...</div> : null}

      <section className="panel historical-hero">
        <div className="section-header">
          <div>
            <p className="section-kicker">Historic Data</p>
            <h2>{historical?.title ?? "Signal 1470 hourly ATSPM run"}</h2>
          </div>
          <p className="section-copy">
            Derived workflow only: original CSVs are read, then filtered/hourly copies and ATSPM result tables are stored separately.
          </p>
        </div>

        {!historical?.exists ? (
          <div className="empty-chart-state">
            <strong>No SQLite run found yet.</strong>
            <p>{historical?.message ?? "Run the hourly backend script to generate this view."}</p>
          </div>
        ) : (
          <>
            <div className="historical-summary-grid">
              <article className="history-stat">
                <span>Signal</span>
                <strong>{historical.signalId}</strong>
              </article>
              <article className="history-stat">
                <span>Date</span>
                <strong>{historical.runDate}</strong>
              </article>
              <article className="history-stat">
                <span>Filtered rows</span>
                <strong>{historical.filteredRows?.toLocaleString()}</strong>
              </article>
              <article className="history-stat">
                <span>Successful hours</span>
                <strong>{historical.summary?.okHours ?? 0}/24</strong>
              </article>
            </div>

            <div className="history-paths">
              <p>
                <strong>Filtered CSV:</strong> {historical.filteredCsvPath}
              </p>
              <p>
                <strong>SQLite DB:</strong> {historical.dbPath}
              </p>
            </div>
          </>
        )}
      </section>

      {historical?.exists ? (
        <>
          <section className="analysis-grid">
            <article className="panel">
              <div className="section-header compact">
                <div>
                  <p className="section-kicker">Hourly slices</p>
                  <h2>Event rows by hour</h2>
                </div>
              </div>
              <div className="chart-wrap">
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={hours}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#d9ddd5" />
                    <XAxis dataKey="label" stroke="#4c5a57" />
                    <YAxis stroke="#4c5a57" />
                    <Tooltip />
                    <Bar dataKey="sourceRows" name="Event rows" fill="#68a77e" radius={[6, 6, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
              {insightBlock(
                "What this shows",
                "The filtered signal-day CSV broken into one file per hour.",
                `${historical.summary?.totalSourceRows.toLocaleString()} source rows were stored across the 24 derived hourly files.`
              )}
            </article>

            <article className="panel">
              <div className="section-header compact">
                <div>
                  <p className="section-kicker">ATSPM outputs</p>
                  <h2>Hourly result snapshot</h2>
                </div>
              </div>
              <div className="chart-wrap">
                <ResponsiveContainer width="100%" height={300}>
                  <LineChart data={hours}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#d9ddd5" />
                    <XAxis dataKey="label" stroke="#4c5a57" />
                    <YAxis yAxisId="left" stroke="#4c5a57" />
                    <YAxis yAxisId="right" orientation="right" stroke="#4c5a57" />
                    <Tooltip />
                    <Legend />
                    <Line yAxisId="left" type="monotone" dataKey="aogPct" name="AOG %" stroke="#4f8864" strokeWidth={3} />
                    <Line yAxisId="right" type="monotone" dataKey="avgWait" name="Avg Wait (s)" stroke="#c98358" strokeWidth={2.6} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
              {insightBlock(
                "What this shows",
                "A quick read of the per-hour ATSPM results stored in SQLite.",
                "This view is intentionally a raw validation view so we can confirm Shawn's library ran hour-by-hour before polishing the analytics."
              )}
            </article>
          </section>

          <section className="table-panel">
            <div className="section-header">
              <div>
                <p className="section-kicker">SQLite</p>
                <h2>Hourly run status</h2>
              </div>
              <p className="section-copy">Each row represents one hourly CSV slice and whether the ATSPM aggregation completed for that slice.</p>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Hour</th>
                    <th>Status</th>
                    <th>Source Rows</th>
                    <th>AOG %</th>
                    <th>Avg Wait (s)</th>
                    <th>Split Failures</th>
                    <th>Max-Outs</th>
                    <th>Gap-Outs</th>
                    <th>Force-Offs</th>
                    <th>Volume Share</th>
                  </tr>
                </thead>
                <tbody>
                  {hours.map((hour) => (
                    <tr key={hour.hour}>
                      <td>{hour.label}</td>
                      <td>
                        <span className={`history-status ${hour.status}`}>{hour.status}</span>
                      </td>
                      <td>{hour.sourceRows.toLocaleString()}</td>
                      <td>{hour.aogPct.toFixed(1)}</td>
                      <td>{hour.avgWait.toFixed(1)}</td>
                      <td>{hour.splitFailures}</td>
                      <td>{hour.maxOuts}</td>
                      <td>{hour.gapOuts}</td>
                      <td>{hour.forceOffs}</td>
                      <td>
                        <span className="volume-bar">
                          <span style={{ width: `${Math.max(2, (hour.sourceRows / maxRows) * 100)}%` }} />
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="panel">
            <div className="section-header compact">
              <div>
                <p className="section-kicker">Stored tables</p>
                <h2>Raw result table counts</h2>
              </div>
            </div>
            <div className="table-count-grid">
              {historical.tableCounts.map((table) => (
                <article key={table.name} className="history-stat">
                  <span>{table.name}</span>
                  <strong>{table.rows.toLocaleString()}</strong>
                </article>
              ))}
            </div>
          </section>
        </>
      ) : null}
    </main>
  );
}

function insightBlock(title: string, what: string, meaning: string) {
  return (
    <div className="chart-notes">
      <p className="chart-what">
        <strong>{title}:</strong> {what}
      </p>
      <p className="chart-meaning">
        <strong>What it means:</strong> {meaning}
      </p>
    </div>
  );
}

export function App() {
  const [dashboard, setDashboard] = useState<DashboardResponse | null>(null);
  const [activeView, setActiveView] = useState<"dashboard" | "historical">("dashboard");
  const [historical, setHistorical] = useState<HistoricalRunResponse | null>(null);
  const [filters, setFilters] = useState<FilterState>({
    dateFrom: "",
    dateTo: "",
    daysOfWeek: [],
    hourFrom: "06:00",
    hourTo: "18:00",
    timeOfDayPreset: "custom",
    selectedPhase: "all"
  });
  const [selectedIntersectionIds, setSelectedIntersectionIds] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [historicalLoading, setHistoricalLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [historicalError, setHistoricalError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();

    async function loadDashboard() {
      setLoading(true);
      setError(null);

      const params = new URLSearchParams({
        dateFrom: filters.dateFrom,
        dateTo: filters.dateTo,
        daysOfWeek: filters.daysOfWeek.join(","),
        hourFrom: filters.hourFrom,
        hourTo: filters.hourTo,
        timeOfDayPreset: filters.timeOfDayPreset,
        phase: filters.selectedPhase,
        intersectionIds: selectedIntersectionIds.join(",")
      });

      try {
        const response = await fetch(`http://127.0.0.1:8000/api/dashboard?${params.toString()}`, {
          signal: controller.signal
        });
        if (!response.ok) {
          throw new Error(`Backend request failed with status ${response.status}`);
        }
        const nextDashboard: DashboardResponse = await response.json();
        setDashboard(nextDashboard);
        setFilters({
          dateFrom: nextDashboard.filters.dateFrom,
          dateTo: nextDashboard.filters.dateTo,
          daysOfWeek: nextDashboard.filters.daysOfWeek,
          hourFrom: nextDashboard.filters.hourFrom,
          hourTo: nextDashboard.filters.hourTo,
          timeOfDayPreset: nextDashboard.filters.timeOfDayPreset,
          selectedPhase: nextDashboard.filters.selectedPhase
        });
        setSelectedIntersectionIds(nextDashboard.filters.selectedIntersectionIds);
      } catch (requestError) {
        if ((requestError as Error).name !== "AbortError") {
          setError((requestError as Error).message);
        }
      } finally {
        setLoading(false);
      }
    }

    loadDashboard();
    return () => controller.abort();
  }, [
    filters.dateFrom,
    filters.dateTo,
    filters.daysOfWeek.join(","),
    filters.hourFrom,
    filters.hourTo,
    filters.timeOfDayPreset,
    filters.selectedPhase,
    selectedIntersectionIds.join(",")
  ]);

  useEffect(() => {
    if (activeView !== "historical") {
      return;
    }
    const controller = new AbortController();

    async function loadHistoricalRun() {
      setHistoricalLoading(true);
      setHistoricalError(null);
      try {
        const response = await fetch("http://127.0.0.1:8000/api/historical-run", {
          signal: controller.signal
        });
        if (!response.ok) {
          throw new Error(`Backend request failed with status ${response.status}`);
        }
        const nextHistorical: HistoricalRunResponse = await response.json();
        setHistorical(nextHistorical);
      } catch (requestError) {
        if ((requestError as Error).name !== "AbortError") {
          setHistoricalError((requestError as Error).message);
        }
      } finally {
        setHistoricalLoading(false);
      }
    }

    loadHistoricalRun();
    return () => controller.abort();
  }, [activeView]);

  const corridor = dashboard?.corridor;
  const intersections = corridor?.intersections ?? [];
  const allIntersections = corridor?.allIntersections ?? [];
  const selectedIntersection = intersections[0];

  const corridorPath = useMemo(
    () => intersections.map((intersection) => [intersection.lat, intersection.lon] as LatLngExpression),
    [intersections]
  );

  const insightMetrics = useMemo(() => {
    if (!corridor || !selectedIntersection) {
      return [];
    }
    const weakest = intersections.reduce((worst, current) =>
      current.summary.meanAog < worst.summary.meanAog ? current : worst
    );
    const allPhaseRows = intersections.flatMap((intersection) =>
      intersection.phases.map((phase) => ({ ...phase, intersection: intersection.name }))
    );
    const worstPhase = allPhaseRows.reduce(
      (worst, current) => (current.avgWait > worst.avgWait ? current : worst),
      allPhaseRows[0] ?? { phase: 0, avgWait: 0, intersection: "No data" }
    );

    return [
      {
        label: "Selection",
        value: `${intersections.length} signals`,
        text: `${filters.dateFrom} to ${filters.dateTo} across ${filters.daysOfWeek.join(", ")}.`,
        icon: "selection" as const
      },
      {
        label: "Weakest AOG",
        value: `${weakest.summary.meanAog.toFixed(1)}%`,
        text: `${weakest.name} has the lowest arrival-on-green in the current selection.`,
        icon: "warning" as const
      },
      {
        label: "Phase pressure",
        value: `P${worstPhase.phase}`,
        text: `${worstPhase.intersection} shows the highest phase wait at ${worstPhase.avgWait.toFixed(1)} s.`,
        icon: "signal" as const
      },
      {
        label: "Cycle",
        value: `${corridor.summary.cycleLength.toFixed(1)} s`,
        text: `${corridor.summary.maxOutCount} max-outs and ${corridor.summary.splitFailures} split failures are in the current window.`,
        icon: "clock" as const
      }
    ];
  }, [corridor, filters.dateFrom, filters.dateTo, filters.daysOfWeek, intersections, selectedIntersection]);

  const phaseChartData = useMemo(() => {
    const rows = intersections.flatMap((intersection) =>
      intersection.phases.map((phase) => ({
        phase: `P${phase.phase}`,
        splitFailures: phase.splitFailures,
        maxOuts: phase.maxOuts,
        gapOuts: phase.gapOuts,
        forceOffs: phase.forceOffs
      }))
    );

    const byPhase = new Map<string, { phase: string; splitFailures: number; maxOuts: number; gapOuts: number; forceOffs: number }>();
    rows.forEach((row) => {
      const current = byPhase.get(row.phase) ?? {
        phase: row.phase,
        splitFailures: 0,
        maxOuts: 0,
        gapOuts: 0,
        forceOffs: 0
      };
      current.splitFailures += row.splitFailures;
      current.maxOuts += row.maxOuts;
      current.gapOuts += row.gapOuts;
      current.forceOffs += row.forceOffs;
      byPhase.set(row.phase, current);
    });
    return Array.from(byPhase.values());
  }, [intersections]);

  const intersectionComparison = useMemo(
    () =>
      intersections.map((intersection) => ({
        name: intersection.name.replace(" & ", " &\n"),
        meanAog: intersection.summary.meanAog,
        meanDelay: intersection.summary.meanDelay
      })),
    [intersections]
  );

  const heatmapLabels = corridor?.trend.map((point) => point.label) ?? [];
  const maxHeat = Math.max(
    1,
    ...intersections.flatMap((intersection) => intersection.trend.map((point) => point.avgWait))
  );

  const glossaryCards = useMemo(
    () =>
      Object.entries(dashboard?.meta.metricDefinitions ?? {}).map(([key, meaning]) => ({
        key,
        title:
          {
            arrivals_on_green: "AOG %",
            average_wait: "Average Wait",
            green_occupancy: "Green Occ %",
            red_occupancy: "Red Occ %",
            cycle_length: "Cycle Length",
            actuation_volume: "Actuation Volume",
            split_failures: "Split Failures",
            max_outs: "Max-Outs",
            gap_outs: "Gap-Outs",
            force_offs: "Force-Offs",
            skips: "Skips",
            phase_green_time: "Green Time"
          }[key] ?? key,
        meaning
      })),
    [dashboard?.meta.metricDefinitions]
  );

  const selectedPreset = dashboard?.meta.timeOfDayPresets.find((preset) => preset.id === filters.timeOfDayPreset);
  const timeOfDayPresets = dashboard?.meta.timeOfDayPresets.length
    ? dashboard.meta.timeOfDayPresets
    : fallbackTimeOfDayPresets;
  const availableDays = dashboard?.meta.availableDays.length ? dashboard.meta.availableDays : fallbackDays;
  const availableDates = dashboard?.meta.availableDates ?? [];
  const maxAvailableDate = availableDates.length ? availableDates[availableDates.length - 1] : undefined;

  return (
    <div className="app-shell">
      <header className="topbar">
        <div>
          <h1>ATSPM Dashboard</h1>
        </div>
        <div className="topbar-note">{dashboard?.meta.dataSource ?? "Loading ATSPM source..."}</div>
      </header>

      <nav className="view-tabs" aria-label="Dashboard views">
        <button
          type="button"
          className={activeView === "dashboard" ? "active" : ""}
          onClick={() => setActiveView("dashboard")}
        >
          Live Dashboard
        </button>
        <button
          type="button"
          className={activeView === "historical" ? "active" : ""}
          onClick={() => setActiveView("historical")}
        >
          Historic Data
        </button>
      </nav>

      {dashboard?.meta.note ? <div className="status-banner">{dashboard.meta.note}</div> : null}
      {error ? <div className="status-banner error">{error}</div> : null}
      {loading && activeView === "dashboard" ? <div className="status-banner">Loading ATSPM aggregates from the selected October files...</div> : null}

      {activeView === "dashboard" ? (
      <main className="workspace">
        <section className="map-panel">
          <div className="section-header">
            <div>
              <p className="section-kicker">Map</p>
              <h2>{corridor?.name ?? "Florida ATSPM map"}</h2>
            </div>
            <p className="section-copy">Choose any mapped signals from the October files to compare operations.</p>
          </div>

          <div className="map-frame">
            <MapContainer
              center={intersections[0] ? [intersections[0].lat, intersections[0].lon] : [28.54, -81.37]}
              zoom={9}
              scrollWheelZoom={false}
              className="leaflet-map"
            >
              <TileLayer
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
              />
              {corridorPath.length > 1 ? (
                <Polyline positions={corridorPath} pathOptions={{ color: "#c98358", weight: 5, opacity: 0.8 }} />
              ) : null}
              {intersections.map((intersection) => (
                <Fragment key={intersection.id}>
                  <Marker position={[intersection.lat, intersection.lon]} icon={markerIcon}>
                    <Popup>
                      <strong>{intersection.name}</strong>
                      <br />
                      Signal ID {intersection.id}
                    </Popup>
                  </Marker>
                  <CircleMarker
                    center={[intersection.lat, intersection.lon]}
                    radius={18}
                    pathOptions={{ color: "#68a77e", opacity: 0.45, weight: 2, fillOpacity: 0.06 }}
                  />
                </Fragment>
              ))}
            </MapContainer>

            <aside className="corridor-list">
              <p className="section-kicker">Intersections</p>
              <p className="list-note">
                {loading
                  ? "Loading mapped signals from the October ATSPM files..."
                  : `${allIntersections.length} mapped signals are available from the current October files.`}
              </p>
              <div className="preset-row">
                {(dashboard?.meta.selectionPresets ?? []).map((preset) => (
                  <button
                    key={preset.id}
                    type="button"
                    className="preset-chip"
                    onClick={() => setSelectedIntersectionIds(preset.intersectionIds.filter((id) => allIntersections.some((item) => item.id === id)))}
                  >
                    {preset.label}
                  </button>
                ))}
              </div>
              {allIntersections.map((intersection, index) => {
                const active = selectedIntersectionIds.includes(intersection.id);
                return (
                  <label key={intersection.id} className={`intersection-row ${active ? "active" : ""}`}>
                    <input
                      type="checkbox"
                      checked={active}
                      onChange={() => {
                        if (active && selectedIntersectionIds.length === 1) {
                          return;
                        }
                        setSelectedIntersectionIds((current) =>
                          current.includes(intersection.id)
                            ? current.filter((id) => id !== intersection.id)
                            : [...current, intersection.id]
                        );
                      }}
                    />
                    <span className="row-index">{String(index + 1).padStart(2, "0")}</span>
                    <span>{intersection.name}</span>
                  </label>
                );
              })}
            </aside>
          </div>
        </section>

        <section className="filters-panel">
          <div className="section-header">
            <div>
              <p className="section-kicker">Filters</p>
              <h2>Analysis window</h2>
            </div>
            <p className="section-copy">Use time-of-day presets or a custom hour range, then narrow to the days you want inside the date window.</p>
          </div>

          <div className="filters-grid filters-grid-v1">
            <label>
              <span>Time of day</span>
              <select
                value={filters.timeOfDayPreset}
                onChange={(event) => {
                  const nextPreset = timeOfDayPresets.find((item) => item.id === event.target.value);
                  setFilters((current) => ({
                    ...current,
                    timeOfDayPreset: event.target.value,
                    hourFrom: nextPreset?.start ?? current.hourFrom,
                    hourTo: nextPreset?.end ?? current.hourTo
                  }));
                }}
              >
                {timeOfDayPresets.map((preset) => (
                  <option key={preset.id} value={preset.id}>
                    {preset.label}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span>Date from</span>
              <input
                type="date"
                value={filters.dateFrom}
                min={availableDates[0]}
                max={maxAvailableDate}
                onChange={(event) => setFilters((current) => ({ ...current, dateFrom: event.target.value }))}
              />
            </label>
            <label>
              <span>Date to</span>
              <input
                type="date"
                value={filters.dateTo}
                min={availableDates[0]}
                max={maxAvailableDate}
                onChange={(event) => setFilters((current) => ({ ...current, dateTo: event.target.value }))}
              />
            </label>
            <label>
              <span>Hour from</span>
              <select
                value={filters.hourFrom}
                onChange={(event) => setFilters((current) => ({ ...current, hourFrom: event.target.value, timeOfDayPreset: "custom" }))}
              >
                {hourOptions.map((hour) => (
                  <option key={hour} value={hour}>
                    {hour}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span>Hour to</span>
              <select
                value={filters.hourTo}
                onChange={(event) => setFilters((current) => ({ ...current, hourTo: event.target.value, timeOfDayPreset: "custom" }))}
              >
                {hourOptions.map((hour) => (
                  <option key={hour} value={hour}>
                    {hour}
                  </option>
                ))}
              </select>
            </label>
            <div className="filter-summary">
              <p className="filter-summary-label">Scope</p>
              <p>{selectedPreset?.label ?? "Custom Period"}</p>
              <strong>
                {selectedIntersectionIds.length} signals, {filters.hourFrom} to {filters.hourTo}
              </strong>
            </div>
          </div>

          <div className="days-row">
            <p className="filter-summary-label">Days of Week</p>
            <div className="day-chip-row">
              {availableDays.map((day) => {
                const active = filters.daysOfWeek.includes(day);
                return (
                  <button
                    key={day}
                    type="button"
                    className={`day-chip ${active ? "active" : ""}`}
                    onClick={() =>
                      setFilters((current) => {
                        if (active && current.daysOfWeek.length === 1) {
                          return current;
                        }
                        return {
                          ...current,
                          daysOfWeek: active
                            ? current.daysOfWeek.filter((item) => item !== day)
                            : [...current.daysOfWeek, day]
                        };
                      })
                    }
                  >
                    {day.slice(0, 2)}
                  </button>
                );
              })}
            </div>
          </div>
        </section>

        <section className="panel phase-operations-panel">
          <div className="section-header">
            <div>
              <p className="section-kicker">Overview</p>
              <h2>Compact ATSPM metrics</h2>
            </div>
            <p className="section-copy">Each metric includes a quick tooltip definition so the dashboard can explain itself while you present it.</p>
          </div>
          <div className="kpi-strip kpi-strip-v1">
            {(corridor?.overviewMetrics ?? []).map((card) => (
              <MetricCardView key={card.id} card={card} />
            ))}
          </div>
        </section>

        <section className="panel phase-operations-panel">
          <div className="section-header">
            <div>
              <p className="section-kicker">Phase Operations</p>
              <h2>Grouped phase measures</h2>
            </div>
            <div className="phase-select-wrap">
              <span className="filter-summary-label">Phase</span>
              <select
                className="phase-select"
                value={filters.selectedPhase}
                onChange={(event) => setFilters((current) => ({ ...current, selectedPhase: event.target.value }))}
              >
                <option value="all">All phases</option>
                {(corridor?.availablePhases ?? []).map((phase) => (
                  <option key={phase} value={String(phase)}>
                    Phase {phase}
                  </option>
                ))}
              </select>
            </div>
          </div>
          <div className="phase-metric-grid">
            {(corridor?.phaseOperationMetrics ?? []).map((card) => (
              <MetricCardView key={card.id} card={card} />
            ))}
          </div>
        </section>

        <section className="analysis-grid">
          <article className="panel">
            <div className="section-header compact">
              <div>
                <p className="section-kicker">Trend</p>
                <h2>AOG and wait over time</h2>
              </div>
            </div>
            <div className="chart-wrap">
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={corridor?.trend ?? []}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#d9ddd5" />
                  <XAxis dataKey="label" stroke="#4c5a57" />
                  <YAxis yAxisId="left" stroke="#4c5a57" />
                  <YAxis yAxisId="right" orientation="right" stroke="#4c5a57" />
                  <Tooltip />
                  <Legend />
                  <Line yAxisId="left" type="monotone" dataKey="aogPct" name="AOG %" stroke="#4f8864" strokeWidth={3} />
                  <Line yAxisId="right" type="monotone" dataKey="avgWait" name="Avg Wait (s)" stroke="#c98358" strokeWidth={2.6} />
                </LineChart>
              </ResponsiveContainer>
            </div>
            {corridor
              ? insightBlock(
                  "What this shows",
                  "Arrival on green and average phase wait tracked across the selected filtered time bins.",
                  corridor.trend.length
                    ? `The trend shows how progression and waiting move together across ${corridor.trend.length} time bins.`
                    : "No trend data is available for the current selection."
                )
              : null}
          </article>

          <article className="panel">
            <div className="section-header compact">
              <div>
                <p className="section-kicker">Operations</p>
                <h2>Phase terminations and failures</h2>
              </div>
            </div>
            <div className="chart-wrap">
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={phaseChartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#d9ddd5" />
                  <XAxis dataKey="phase" stroke="#4c5a57" />
                  <YAxis stroke="#4c5a57" />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="splitFailures" fill="#c98358" radius={[6, 6, 0, 0]} />
                  <Bar dataKey="maxOuts" fill="#7b3032" radius={[6, 6, 0, 0]} />
                  <Bar dataKey="gapOuts" fill="#68a77e" radius={[6, 6, 0, 0]} />
                  <Bar dataKey="forceOffs" fill="#3f654c" radius={[6, 6, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
            {insightBlock(
              "What this shows",
              "A phase-by-phase view of split failures and termination behavior across the selected signals.",
              phaseChartData.length
                ? "This shows which phases are ending by demand exhaustion versus gap-out or coordination force-off."
                : "No phase operation counts are available for the current selection."
            )}
          </article>

          <article className="panel">
            <div className="section-header compact">
              <div>
                <p className="section-kicker">Comparison</p>
                <h2>Signal comparison</h2>
              </div>
            </div>
            <div className="chart-wrap">
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={intersectionComparison}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#d9ddd5" />
                  <XAxis dataKey="name" stroke="#4c5a57" interval={0} tick={{ fontSize: 11 }} />
                  <YAxis stroke="#4c5a57" />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="meanAog" name="AOG %" fill="#4f8864" radius={[6, 6, 0, 0]} />
                  <Bar dataKey="meanDelay" name="Avg Wait (s)" fill="#c98358" radius={[6, 6, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
            {insightBlock(
              "What this shows",
              "A side-by-side comparison of selected intersections using arrival-on-green and average wait.",
              intersectionComparison.length
                ? "This shows which selected signal is serving arrivals better versus which one is carrying more wait pressure."
                : "No signal comparison data is available for the current selection."
            )}
          </article>

          <article className="panel insights-panel">
            <div className="section-header compact">
              <div>
                <p className="section-kicker">Insights</p>
                <h2>Auto-generated notes</h2>
              </div>
            </div>
            <div className="insight-grid">
              {insightMetrics.map((item) => (
                <article key={item.label} className="insight-card">
                  <div className="insight-top">
                    <div className="insight-heading">
                      <span className="insight-icon">
                        <UiIcon kind={item.icon} />
                      </span>
                      <span className="insight-label">{item.label}</span>
                    </div>
                    <strong className="insight-value">{item.value}</strong>
                  </div>
                  <p>{item.text}</p>
                </article>
              ))}
            </div>
          </article>
        </section>

        <section className="panel heatmap-panel">
          <div className="section-header compact">
            <div>
              <p className="section-kicker">Heatmap</p>
              <h2>Wait intensity by signal and time bucket</h2>
            </div>
          </div>
          <div className="heatmap-table">
            <div className="heatmap-header heatmap-corner">Intersection</div>
            {heatmapLabels.map((label) => (
              <div key={`header-${label}`} className="heatmap-header">
                {label}
              </div>
            ))}
            {intersections.map((intersection) => (
              <Fragment key={intersection.id}>
                <div className="heatmap-row-label">{intersection.name}</div>
                {intersection.trend.map((point) => {
                  const intensity = point.avgWait / maxHeat;
                  return (
                    <div
                      key={`${intersection.id}-${point.label}`}
                      className="heatmap-value"
                      style={{ backgroundColor: `rgba(201, 131, 88, ${0.12 + intensity * 0.6})` }}
                    >
                      <strong>{point.avgWait.toFixed(1)}</strong>
                    </div>
                  );
                })}
              </Fragment>
            ))}
          </div>
          {insightBlock(
            "What this shows",
            "Average wait intensity across the selected signals and time buckets.",
            heatmapLabels.length
              ? "This shows whether the pressure is concentrated at one signal or recurring across the selected row."
              : "No heatmap data is available for the current selection."
          )}
        </section>

        <section className="table-panel">
          <div className="section-header">
            <div>
              <p className="section-kicker">Phases</p>
              <h2>Performance indices</h2>
            </div>
            <p className="section-copy">Phase-level ATSPM measures for the current filtered selection.</p>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Intersection</th>
                  <th>Phase</th>
                  <th>AOG %</th>
                  <th>Avg Wait (s)</th>
                  <th>Green Occ %</th>
                  <th>Red Occ %</th>
                  <th>Split Failures</th>
                  <th>Max-Outs</th>
                  <th>Gap-Outs</th>
                  <th>Force-Offs</th>
                  <th>Skips</th>
                </tr>
              </thead>
              <tbody>
                {intersections.flatMap((intersection) =>
                  intersection.phases.map((phase) => (
                    <tr key={`${intersection.id}-${phase.phase}`}>
                      <td>{intersection.name}</td>
                      <td>{phase.phase}</td>
                      <td>{phase.aogPct.toFixed(1)}</td>
                      <td>{phase.avgWait.toFixed(1)}</td>
                      <td>{phase.greenOccupancyPct.toFixed(1)}</td>
                      <td>{phase.redOccupancyPct.toFixed(1)}</td>
                      <td>{phase.splitFailures}</td>
                      <td>{phase.maxOuts}</td>
                      <td>{phase.gapOuts}</td>
                      <td>{phase.forceOffs}</td>
                      <td>{phase.skips}</td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </section>

        <section className="panel glossary-panel">
          <div className="section-header compact">
            <div>
              <p className="section-kicker">Definitions</p>
              <h2>Metric key</h2>
            </div>
          </div>
          <div className="glossary-grid">
            {glossaryCards.map((item) => (
              <article key={item.key} className="glossary-item">
                <div className="glossary-top">
                  <span className="glossary-icon">
                    <UiIcon kind="book" />
                  </span>
                  <strong>{item.title}</strong>
                </div>
                <p>{item.meaning}</p>
              </article>
            ))}
          </div>
        </section>
      </main>
      ) : (
        <HistoricalRunView historical={historical} loading={historicalLoading} error={historicalError} />
      )}
    </div>
  );
}
