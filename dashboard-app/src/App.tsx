import { Fragment, useEffect, useMemo, useState, type Dispatch, type SetStateAction } from "react";
import { CircleMarker, MapContainer, Marker, Polyline, Popup, TileLayer } from "react-leaflet";
import { divIcon, LatLngExpression } from "leaflet";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  Scatter,
  ScatterChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis
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
  runDate: string;
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

type HistoricalDay = {
  runDate: string;
  label: string;
  sourceRows: number;
  okHours: number;
  emptyHours: number;
  failedHours: number;
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
  selectedSignalId?: string;
  availableSignals: Array<{
    id: string;
    name: string;
    route: string;
    region: string;
    lat: number;
    lon: number;
  }>;
  runDate?: string;
  sourceCsv?: string;
  dbPath: string;
  outputDir: string;
  filteredCsvPath: string;
  filteredRows?: number;
  detectorConfigRows?: number;
  createdAt?: string;
  days: HistoricalDay[];
  summary?: {
    okHours: number;
    emptyHours: number;
    failedHours: number;
    totalSourceRows: number;
  };
  hours: HistoricalHour[];
  tableCounts: Array<{ name: string; rows: number }>;
};

type RankingRow = {
  id: string;
  name: string;
  route: string;
  region: string;
  hasHistoricalDb: boolean;
  hasData: boolean;
  aogPct: number | null;
  aorPct: number | null;
  avgWait: number | null;
  greenOccupancyPct: number | null;
  redOccupancyPct: number | null;
  splitFailures: number | null;
  maxOuts: number | null;
  forceOffs: number | null;
};

type RankingResponse = {
  meta: {
    dataSource: string;
    builtSignals: number;
    signalsWithData: number;
    availableSignals: number;
    metricDefinitions: Record<string, string>;
  };
  filters: {
    dateFrom: string;
    dateTo: string;
    daysOfWeek: string[];
    hourFrom: string;
    hourTo: string;
    timeOfDayPreset: string;
  };
  rows: RankingRow[];
};

type DayHourCell = {
  dayName: string;
  hour: number;
  label: string;
  value: number | null;
  sampleCount: number;
};

type DayHourResponse = {
  meta: {
    dataSource: string;
    metricDefinitions: Record<string, string>;
    availableSignals: Array<{
      id: string;
      name: string;
      route: string;
      region: string;
      lat: number;
      lon: number;
    }>;
  };
  filters: {
    signalId: string;
    metric: Exclude<RankingMetricKey, "compositeScore">;
    dateFrom: string;
    dateTo: string;
    daysOfWeek: string[];
    hourFrom: string;
    hourTo: string;
    timeOfDayPreset: string;
  };
  signal: {
    id: string;
    name: string;
    route: string;
    region: string;
    lat: number;
    lon: number;
  };
  hours: string[];
  days: string[];
  cells: DayHourCell[];
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

type RankedIntersection = IntersectionPayload & {
  rank: number | null;
  score: number | null;
  category: "best" | "watch" | "alert" | "nodata";
};

type RankingMetricKey =
  | "aogPct"
  | "aorPct"
  | "avgWait"
  | "greenOccupancyPct"
  | "redOccupancyPct"
  | "splitFailures"
  | "maxOuts"
  | "forceOffs"
  | "compositeScore";

function buildMarkerIcon(tone: RankedIntersection["category"], active: boolean) {
  return divIcon({
    className: "intersection-pin-shell",
    html: `<span class="map-pin map-pin--${tone}${active ? " map-pin--active" : ""}"></span>`,
    iconSize: [24, 24],
    iconAnchor: [12, 12]
  });
}

function quantile(sortedValues: number[], percentile: number) {
  if (!sortedValues.length) {
    return 0;
  }
  const index = (sortedValues.length - 1) * percentile;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) {
    return sortedValues[lower];
  }
  const weight = index - lower;
  return sortedValues[lower] * (1 - weight) + sortedValues[upper] * weight;
}

function quartileThresholds(values: number[]) {
  const sorted = [...values].sort((left, right) => left - right);
  return {
    q1: quantile(sorted, 0.25),
    q2: quantile(sorted, 0.5),
    q3: quantile(sorted, 0.75)
  };
}

function bucketFromThresholds(value: number | null, thresholds: { q1: number; q2: number; q3: number }) {
  if (value === null || Number.isNaN(value)) {
    return null;
  }
  if (value <= thresholds.q1) {
    return 0;
  }
  if (value <= thresholds.q2) {
    return 1;
  }
  if (value <= thresholds.q3) {
    return 2;
  }
  return 3;
}

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
  error,
  historicalSignalId,
  onSignalChange
}: {
  historical: HistoricalRunResponse | null;
  loading: boolean;
  error: string | null;
  historicalSignalId: string;
  onSignalChange: (value: string) => void;
}) {
  const matchesSelectedSignal =
    historical !== null &&
    String(historical.selectedSignalId ?? historical.signalId ?? "") === historicalSignalId;
  const activeHistorical = matchesSelectedSignal ? historical : null;
  const days = activeHistorical?.days ?? [];
  const hours = activeHistorical?.hours ?? [];
  const maxDayRows = Math.max(1, ...days.map((day) => day.sourceRows));
  const maxHeat = Math.max(1, ...hours.map((hour) => hour.avgWait));
  const hourLabels = Array.from({ length: 24 }, (_, hour) => `${String(hour).padStart(2, "0")}:00`);
  const signalOptions = historical?.availableSignals ?? [];
  const selectedSignalOption = signalOptions.find((signal) => signal.id === historicalSignalId);
  const historicalHeading = selectedSignalOption
    ? `${selectedSignalOption.name} historical ATSPM run`
    : activeHistorical?.title ?? "Historical ATSPM run";
  const historicalMapCenter: LatLngExpression = [28.54, -81.37];
  const hourlyByDay = new Map(
    days.map((day) => [
      day.runDate,
      hourLabels.map((label, index) => hours.find((hour) => hour.runDate === day.runDate && hour.hour === index) ?? null)
    ])
  );
  const bestAogDay = days.reduce((best, current) => (current.aogPct > best.aogPct ? current : best), days[0] ?? null);
  const worstWaitDay = days.reduce((worst, current) => (current.avgWait > worst.avgWait ? current : worst), days[0] ?? null);

  return (
    <main className="workspace historical-workspace">
      {error ? <div className="status-banner error">{error}</div> : null}
      {loading || !matchesSelectedSignal ? (
        <div className="status-banner">Loading the hourly SQLite run summary for signal {historicalSignalId}...</div>
      ) : null}

      <section className="panel historical-hero">
        <div className="section-header">
          <div>
            <p className="section-kicker">Historic Data</p>
            <h2>{historicalHeading}</h2>
          </div>
        </div>

        <div className="historical-top-frame">
          <div className="historical-map-card">
            <p className="section-kicker">Map</p>
            <div className="historical-map-frame">
              <MapContainer
                key={`historical-map-${signalOptions.length}`}
                center={historicalMapCenter}
                zoom={8}
                scrollWheelZoom={false}
                className="leaflet-map historical-leaflet-map"
              >
                <TileLayer
                  attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                  url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                />
                {signalOptions.map((signal) => {
                  const active = signal.id === historicalSignalId;
                  return (
                    <Fragment key={`historical-signal-${signal.id}`}>
                      <Marker
                        position={[signal.lat, signal.lon]}
                        icon={buildMarkerIcon("watch", active)}
                        eventHandlers={{ click: () => onSignalChange(signal.id) }}
                      >
                        <Popup>
                          <strong>{signal.name}</strong>
                          <br />
                          Signal ID {signal.id}
                          <br />
                          {active ? "Selected in historical view" : "Click marker to select"}
                        </Popup>
                      </Marker>
                      {active ? (
                        <CircleMarker
                          center={[signal.lat, signal.lon]}
                          radius={18}
                          pathOptions={{ color: "#68a77e", opacity: 0.7, weight: 3, fillOpacity: 0.1 }}
                        />
                      ) : null}
                    </Fragment>
                  );
                })}
              </MapContainer>
            </div>
          </div>

          <div className="historical-select-wrap">
            <label>
              <span className="filter-summary-label">Intersection</span>
              <select value={historicalSignalId} onChange={(event) => onSignalChange(event.target.value)}>
                {(historical?.availableSignals ?? []).map((signal) => (
                  <option key={signal.id} value={signal.id}>
                    {signal.id} · {signal.name}
                  </option>
                ))}
              </select>
            </label>
            <p className="section-copy">
              Derived workflow only: original CSVs are read, then filtered/hourly copies and ATSPM result tables are stored separately.
            </p>
          </div>
        </div>

        {!activeHistorical?.exists ? (
          <div className="empty-chart-state">
            <strong>Historical run not generated yet.</strong>
            <p>{activeHistorical?.message ?? "This signal has not finished building its derived hourly historical tables yet."}</p>
          </div>
        ) : activeHistorical.exists && days.length === 0 ? (
          <div className="empty-chart-state">
            <strong>No October historical rows were found for this signal.</strong>
            <p>The derived historical database exists, but the current October source files did not produce day-level results for this intersection.</p>
          </div>
        ) : (
          <>
            <div className="historical-summary-grid">
              <article className="history-stat">
                <span>Signal</span>
                <strong>{activeHistorical?.signalId ?? historicalSignalId}</strong>
              </article>
              <article className="history-stat">
                <span>Coverage</span>
                <strong>{days.length} days</strong>
              </article>
              <article className="history-stat">
                <span>Total rows</span>
                <strong>{activeHistorical?.filteredRows?.toLocaleString()}</strong>
              </article>
              <article className="history-stat">
                <span>Successful hours</span>
                <strong>{activeHistorical?.summary?.okHours ?? 0}/{hours.length || 0}</strong>
              </article>
            </div>

            <div className="history-paths">
              <p>
                <strong>Best AOG day:</strong> {bestAogDay ? `${bestAogDay.runDate} (${bestAogDay.aogPct.toFixed(1)}%)` : "No data"}
              </p>
              <p>
                <strong>Worst wait day:</strong> {worstWaitDay ? `${worstWaitDay.runDate} (${worstWaitDay.avgWait.toFixed(1)} s)` : "No data"}
              </p>
            </div>
          </>
        )}
      </section>

      {activeHistorical?.exists && days.length > 0 ? (
        <>
          <section className="analysis-grid">
            <article className="panel">
              <div className="section-header compact">
                <div>
                  <p className="section-kicker">Daily delay</p>
                  <h2>Average wait by day</h2>
                </div>
              </div>
              <div className="chart-wrap">
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={days}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#d9ddd5" />
                    <XAxis dataKey="label" stroke="#4c5a57" />
                    <YAxis stroke="#4c5a57" />
                    <Tooltip />
                    <Bar dataKey="avgWait" name="Avg Wait (s)" fill="#c98358" radius={[6, 6, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
              {insightBlock(
                "What this shows",
                "Average phase wait for signal 1470, summarized one day at a time.",
                worstWaitDay
                  ? `${worstWaitDay.runDate} has the highest daily wait at ${worstWaitDay.avgWait.toFixed(1)} seconds.`
                  : "No daily wait data is available."
              )}
            </article>

            <article className="panel">
              <div className="section-header compact">
                <div>
                  <p className="section-kicker">Progression</p>
                  <h2>AOG and split failures by day</h2>
                </div>
              </div>
              <div className="chart-wrap">
                <ResponsiveContainer width="100%" height={300}>
                  <LineChart data={days}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#d9ddd5" />
                    <XAxis dataKey="label" stroke="#4c5a57" />
                    <YAxis yAxisId="left" stroke="#4c5a57" />
                    <YAxis yAxisId="right" orientation="right" stroke="#4c5a57" />
                    <Tooltip />
                    <Legend />
                    <Line yAxisId="left" type="monotone" dataKey="aogPct" name="AOG %" stroke="#4f8864" strokeWidth={3} />
                    <Line yAxisId="right" type="monotone" dataKey="splitFailures" name="Split Failures" stroke="#ae633d" strokeWidth={2.6} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
              {insightBlock(
                "What this shows",
                "Daily arrivals on green compared against daily split failures.",
                bestAogDay
                  ? `${bestAogDay.runDate} has the strongest daily AOG at ${bestAogDay.aogPct.toFixed(1)}%.`
                  : "No daily progression data is available."
              )}
            </article>
          </section>

          <section className="analysis-grid">
            <article className="panel">
              <div className="section-header compact">
                <div>
                  <p className="section-kicker">Terminations</p>
                  <h2>Daily phase endings</h2>
                </div>
              </div>
              <div className="chart-wrap">
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={days}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#d9ddd5" />
                    <XAxis dataKey="label" stroke="#4c5a57" />
                    <YAxis stroke="#4c5a57" />
                    <Tooltip />
                    <Legend />
                    <Bar dataKey="maxOuts" name="Max-Outs" fill="#7b3032" radius={[6, 6, 0, 0]} />
                    <Bar dataKey="gapOuts" name="Gap-Outs" fill="#68a77e" radius={[6, 6, 0, 0]} />
                    <Bar dataKey="forceOffs" name="Force-Offs" fill="#3f654c" radius={[6, 6, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
              {insightBlock(
                "What this shows",
                "Daily termination totals showing whether phases ended by max-out, gap-out, or force-off.",
                "This shows whether the controller behavior was more demand-driven or coordination-driven on each day."
              )}
            </article>

            <article className="panel heatmap-panel">
              <div className="section-header compact">
                <div>
                  <p className="section-kicker">Heatmap</p>
                  <h2>Wait by day and hour</h2>
                </div>
              </div>
              <div className="history-heatmap">
                <div className="heatmap-header heatmap-corner">Date</div>
                {hourLabels.map((label) => (
                  <div key={`history-header-${label}`} className="heatmap-header">
                    {label}
                  </div>
                ))}
                {days.map((day) => (
                  <Fragment key={`history-${day.runDate}`}>
                    <div className="heatmap-row-label">{day.runDate}</div>
                    {(hourlyByDay.get(day.runDate) ?? []).map((hour, index) => (
                      <div
                        key={`${day.runDate}-${index}`}
                        className="heatmap-value"
                        style={{
                          backgroundColor: `rgba(201, 131, 88, ${
                            hour ? 0.08 + (hour.avgWait / maxHeat) * 0.72 : 0.04
                          })`
                        }}
                      >
                        <strong>{hour ? hour.avgWait.toFixed(0) : "-"}</strong>
                      </div>
                    ))}
                  </Fragment>
                ))}
              </div>
              {insightBlock(
                "What this shows",
                "Average wait intensity by day and hour for the historical run.",
                "This shows exactly when the worst pressure appears instead of only averaging the whole day together."
              )}
            </article>
          </section>

          <section className="table-panel">
            <div className="section-header">
              <div>
                <p className="section-kicker">SQLite</p>
                <h2>Daily run status</h2>
              </div>
              <p className="section-copy">Each row represents one day from the 1470 historical run and how many hourly slices were populated or empty.</p>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Date</th>
                    <th>Source Rows</th>
                    <th>Ok Hours</th>
                    <th>Empty Hours</th>
                    <th>Failed Hours</th>
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
                  {days.map((day) => (
                    <tr key={day.runDate}>
                      <td>{day.runDate}</td>
                      <td>{day.sourceRows.toLocaleString()}</td>
                      <td>{day.okHours}</td>
                      <td>{day.emptyHours}</td>
                      <td>{day.failedHours}</td>
                      <td>{day.aogPct.toFixed(1)}</td>
                      <td>{day.avgWait.toFixed(1)}</td>
                      <td>{day.splitFailures}</td>
                      <td>{day.maxOuts}</td>
                      <td>{day.gapOuts}</td>
                      <td>{day.forceOffs}</td>
                      <td>
                        <span className="volume-bar">
                          <span style={{ width: `${Math.max(2, (day.sourceRows / maxDayRows) * 100)}%` }} />
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="table-panel">
            <div className="section-header">
              <div>
                <p className="section-kicker">Stored tables</p>
                <h2>Raw result table counts</h2>
              </div>
              <p className="section-copy">A compact count of the generated ATSPM result tables behind this historical signal run.</p>
            </div>
            <div className="table-count-grid">
              {activeHistorical.tableCounts.map((table) => (
                <article key={table.name} className="history-stat">
                  <span>{table.name}</span>
                  <strong>{table.rows.toLocaleString()}</strong>
                </article>
              ))}
            </div>
          </section>

          <section className="table-panel">
            <div className="section-header">
              <div>
                <p className="section-kicker">Hourly detail</p>
                <h2>Per-hour validation rows</h2>
              </div>
              <p className="section-copy">This is the raw hour-level validation table behind the day summaries above.</p>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Date</th>
                    <th>Hour</th>
                    <th>Status</th>
                    <th>Source Rows</th>
                    <th>AOG %</th>
                    <th>Avg Wait (s)</th>
                    <th>Split Failures</th>
                    <th>Max-Outs</th>
                    <th>Gap-Outs</th>
                    <th>Force-Offs</th>
                  </tr>
                </thead>
                <tbody>
                  {hours.map((hour) => (
                    <tr key={`${hour.runDate}-${hour.hour}`}>
                      <td>{hour.runDate}</td>
                      <td>{String(hour.hour).padStart(2, "0")}:00</td>
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
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        </>
      ) : null}
    </main>
  );
}

function RankingView({
  ranking,
  loading,
  error
}: {
  ranking: RankingResponse | null;
  loading: boolean;
  error: string | null;
}) {
  const [sortMetric, setSortMetric] = useState<RankingMetricKey>("compositeScore");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc");
  const dataRows = useMemo(() => {
    const usable = (ranking?.rows ?? []).filter((row) => row.hasData);
    const maxWait = Math.max(1, ...usable.map((row) => row.avgWait ?? 0));
    const maxSplit = Math.max(1, ...usable.map((row) => row.splitFailures ?? 0));
    const maxMaxOuts = Math.max(1, ...usable.map((row) => row.maxOuts ?? 0));
    const maxForceOffs = Math.max(1, ...usable.map((row) => row.forceOffs ?? 0));

    return usable
      .map((row) => {
        const compositeScore =
          ((row.aogPct ?? 0) / 100) * 0.3 +
          (1 - (row.aorPct ?? 100) / 100) * 0.1 +
          (1 - (row.avgWait ?? maxWait) / maxWait) * 0.2 +
          (1 - (row.splitFailures ?? maxSplit) / maxSplit) * 0.15 +
          (1 - (row.maxOuts ?? maxMaxOuts) / maxMaxOuts) * 0.15 +
          (1 - (row.forceOffs ?? maxForceOffs) / maxForceOffs) * 0.1;

        return { ...row, compositeScore: Math.max(0, Math.min(100, compositeScore * 100)) };
      })
      .sort((left, right) => right.compositeScore - left.compositeScore);
  }, [ranking?.rows]);

  const noDataRows = useMemo(
    () => (ranking?.rows ?? []).filter((row) => !row.hasData).map((row) => ({ ...row, compositeScore: null })),
    [ranking?.rows]
  );

  const rankedRows = useMemo(() => {
    const thresholds = quartileThresholds(dataRows.map((row) => row.compositeScore));
    const classified = dataRows.map((row, index) => {
      const bucket = bucketFromThresholds(row.compositeScore, thresholds);
      const classification =
        bucket === 3
          ? "Top Quartile"
          : bucket === 2
            ? "Upper-Mid"
            : bucket === 1
              ? "Lower-Mid"
              : "Bottom Quartile";
      return {
        ...row,
        rank: index + 1,
        classification
      };
    });
    return [...classified, ...noDataRows.map((row) => ({ ...row, rank: null, classification: "No data" }))];
  }, [dataRows, noDataRows]);

  const sortedRows = useMemo(() => {
    const withData = rankedRows.filter((row) => row.hasData);
    const withoutData = rankedRows.filter((row) => !row.hasData);
    const sortedWithData = [...withData].sort((left, right) => {
      const leftValue = left[sortMetric as keyof typeof left];
      const rightValue = right[sortMetric as keyof typeof right];
      const leftNumber = typeof leftValue === "number" ? leftValue : Number.POSITIVE_INFINITY;
      const rightNumber = typeof rightValue === "number" ? rightValue : Number.POSITIVE_INFINITY;
      if (leftNumber === rightNumber) {
        return left.name.localeCompare(right.name);
      }
      return sortDirection === "asc" ? leftNumber - rightNumber : rightNumber - leftNumber;
    });
    return [...sortedWithData, ...withoutData];
  }, [rankedRows, sortMetric, sortDirection]);

  const metricColors: Record<RankingMetricKey, string> = {
    aogPct: "79, 136, 100",
    aorPct: "176, 103, 70",
    avgWait: "201, 131, 88",
    greenOccupancyPct: "81, 129, 114",
    redOccupancyPct: "152, 94, 108",
    splitFailures: "143, 68, 60",
    maxOuts: "120, 92, 148",
    forceOffs: "98, 111, 86",
    compositeScore: "58, 108, 79"
  };

  const thresholdsByMetric = useMemo(() => {
    const metricValues = {
      aogPct: rankedRows.map((row) => row.aogPct).filter((value): value is number => value !== null),
      aorPct: rankedRows.map((row) => row.aorPct).filter((value): value is number => value !== null),
      avgWait: rankedRows.map((row) => row.avgWait).filter((value): value is number => value !== null),
      greenOccupancyPct: rankedRows.map((row) => row.greenOccupancyPct).filter((value): value is number => value !== null),
      redOccupancyPct: rankedRows.map((row) => row.redOccupancyPct).filter((value): value is number => value !== null),
      splitFailures: rankedRows.map((row) => row.splitFailures).filter((value): value is number => value !== null),
      maxOuts: rankedRows.map((row) => row.maxOuts).filter((value): value is number => value !== null),
      forceOffs: rankedRows.map((row) => row.forceOffs).filter((value): value is number => value !== null),
      compositeScore: rankedRows.map((row) => row.compositeScore).filter((value): value is number => value !== null)
    };

    return Object.fromEntries(
      Object.entries(metricValues).map(([key, values]) => [key, quartileThresholds(values)])
    ) as Record<RankingMetricKey, { q1: number; q2: number; q3: number }>;
  }, [rankedRows]);

  const cellStyle = (metric: RankingMetricKey, value: number | null) => {
    if (value === null) {
      return undefined;
    }
    const bucket = bucketFromThresholds(value, thresholdsByMetric[metric]);
    const alpha = [0.12, 0.24, 0.42, 0.62][bucket ?? 0];
    return {
      backgroundColor: `rgba(${metricColors[metric]}, ${alpha})`,
      borderColor: `rgba(${metricColors[metric]}, 0.7)`
    };
  };

  const formatValue = (value: number | null, suffix = "", digits = 1) =>
    value === null ? "—" : `${value.toFixed(digits)}${suffix}`;

  const metricDefinitions = ranking?.meta.metricDefinitions ?? {};

  const applySort = (metric: RankingMetricKey) => {
    if (metric === sortMetric) {
      setSortDirection((current) => (current === "asc" ? "desc" : "asc"));
      return;
    }
    setSortMetric(metric);
    setSortDirection("asc");
  };

  const sortIndicator = (metric: RankingMetricKey) => {
    if (metric !== sortMetric) {
      return "↕";
    }
    return sortDirection === "asc" ? "↑" : "↓";
  };

  const topBest = useMemo(() => rankedRows.filter((row) => row.hasData).slice(0, 10), [rankedRows]);
  const topWorst = useMemo(
    () =>
      [...rankedRows]
        .filter((row) => row.hasData)
        .sort((left, right) => (left.compositeScore ?? 0) - (right.compositeScore ?? 0))
        .slice(0, 10),
    [rankedRows]
  );
  const classSummary = useMemo(() => {
    const counts = new Map<string, number>();
    rankedRows.forEach((row) => {
      counts.set(row.classification, (counts.get(row.classification) ?? 0) + 1);
    });
    return [
      "Top Quartile",
      "Upper-Mid",
      "Lower-Mid",
      "Bottom Quartile",
      "No data"
    ].map((label) => ({ label, count: counts.get(label) ?? 0 }));
  }, [rankedRows]);

  const scatterData = useMemo(
    () =>
      dataRows.map((row) => ({
        name: row.name,
        id: row.id,
        compositeScore: Number(row.compositeScore.toFixed(1)),
        avgWait: Number((row.avgWait ?? 0).toFixed(1)),
        splitFailures: row.splitFailures ?? 0,
        bubbleSize: Math.max(80, (row.splitFailures ?? 0) * 4 + (row.maxOuts ?? 0) * 10 + 80)
      })),
    [dataRows]
  );

  const renderHeader = (
    metric: RankingMetricKey,
    label: string
  ) => (
    <div className="ranking-header-cell">
      <button type="button" className={`ranking-sort-button ${sortMetric === metric ? "active" : ""}`} onClick={() => applySort(metric)}>
        {label} <span>{sortIndicator(metric)}</span>
      </button>
      {metricDefinitions[metric] ? <MetricInfo description={metricDefinitions[metric]} /> : null}
    </div>
  );

  return (
    <>
      {error ? <div className="status-banner error">{error}</div> : null}
      {loading ? <div className="status-banner">Loading SQLite-based ranking metrics across all historical signals...</div> : null}

      <section className="panel">
        <div className="section-header">
          <div>
            <p className="section-kicker">Ranking</p>
            <h2>Intersection classification matrix</h2>
          </div>
          <p className="section-copy">
            Rows are intersections and columns are MOEs from the historical SQLite outputs. Darker cells mean the value is in a higher quartile for that MOE.
          </p>
        </div>

        <div className="historical-summary-grid">
          <article className="history-stat">
            <span>Mapped signals</span>
            <strong>{ranking?.meta.availableSignals ?? 0}</strong>
          </article>
          <article className="history-stat">
            <span>Built SQLite DBs</span>
            <strong>{ranking?.meta.builtSignals ?? 0}</strong>
          </article>
          <article className="history-stat">
            <span>Signals with data</span>
            <strong>{ranking?.meta.signalsWithData ?? 0}</strong>
          </article>
          <article className="history-stat">
            <span>Window</span>
            <strong>{ranking ? `${ranking.filters.hourFrom} to ${ranking.filters.hourTo}` : "—"}</strong>
          </article>
        </div>
      </section>

      <section className="ranking-summary-grid">
        <article className="panel ranking-list-panel">
          <div className="section-header compact">
            <div>
              <p className="section-kicker">Top 10 Best</p>
              <h2>Best composite performers</h2>
            </div>
            <p className="section-copy">Highest overall score for the selected days and hour range.</p>
          </div>
          <div className="ranking-mini-list">
            {topBest.map((row) => (
              <div key={`best-${row.id}`} className="ranking-mini-row">
                <span className="ranking-mini-rank">#{row.rank}</span>
                <div>
                  <strong>{row.name}</strong>
                  <span>{formatValue(row.compositeScore, "", 0)} composite</span>
                </div>
              </div>
            ))}
          </div>
        </article>

        <article className="panel ranking-list-panel">
          <div className="section-header compact">
            <div>
              <p className="section-kicker">Top 10 Worst</p>
              <h2>Lowest composite performers</h2>
            </div>
            <p className="section-copy">Lowest overall score for the selected days and hour range.</p>
          </div>
          <div className="ranking-mini-list">
            {topWorst.map((row) => (
              <div key={`worst-${row.id}`} className="ranking-mini-row">
                <span className="ranking-mini-rank">#{row.rank}</span>
                <div>
                  <strong>{row.name}</strong>
                  <span>{formatValue(row.compositeScore, "", 0)} composite</span>
                </div>
              </div>
            ))}
          </div>
        </article>

        <article className="panel ranking-class-panel">
          <div className="section-header compact">
            <div>
              <p className="section-kicker">Class Summary</p>
              <h2>Intersection category count</h2>
            </div>
            <p className="section-copy">Quick distribution of how the current filtered set is classified.</p>
          </div>
          <div className="ranking-class-grid">
            {classSummary.map((item) => (
              <div key={item.label} className="ranking-class-card">
                <span>{item.label}</span>
                <strong>{item.count}</strong>
              </div>
            ))}
          </div>
        </article>
      </section>

      <section className="panel">
        <div className="section-header">
          <div>
            <p className="section-kicker">Scatter / Bubble</p>
            <h2>Composite score vs average wait</h2>
          </div>
          <p className="section-copy">Bubble size tracks split failures, so larger circles highlight intersections carrying both low performance and operational pressure.</p>
        </div>
        <div className="chart-wrap">
          <ResponsiveContainer width="100%" height={340}>
            <ScatterChart margin={{ top: 10, right: 20, bottom: 16, left: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#d9ddd5" />
              <XAxis
                type="number"
                dataKey="compositeScore"
                name="Composite score"
                stroke="#4c5a57"
                domain={[0, 100]}
                label={{ value: "Composite score", position: "insideBottom", offset: -6 }}
              />
              <YAxis
                type="number"
                dataKey="avgWait"
                name="Average wait"
                stroke="#4c5a57"
                label={{ value: "Average wait (s)", angle: -90, position: "insideLeft" }}
              />
              <ZAxis type="number" dataKey="bubbleSize" range={[80, 420]} />
              <Tooltip
                formatter={(value: number, name: string) => {
                  if (name === "avgWait") {
                    return [`${value.toFixed(1)} s`, "Average wait"];
                  }
                  if (name === "compositeScore") {
                    return [`${value.toFixed(1)}`, "Composite score"];
                  }
                  if (name === "splitFailures") {
                    return [String(value), "Split failures"];
                  }
                  return [String(value), name];
                }}
                contentStyle={{ borderRadius: 14, border: "2px solid #c77e57", backgroundColor: "#fffdf9" }}
                cursor={{ strokeDasharray: "4 4" }}
              />
              <Scatter data={scatterData} fill="#5b8f69" />
            </ScatterChart>
          </ResponsiveContainer>
        </div>
      </section>

      <section className="table-panel ranking-table-panel">
        <div className="section-header">
          <div>
            <p className="section-kicker">MOE Table</p>
            <h2>Quartile-colored intersection ranking</h2>
          </div>
          <p className="section-copy">
            Composite score blends AOG, AOR, average wait, split failures, max-outs, and force-offs from the filtered historical SQLite data.
          </p>
        </div>
        <div className="table-wrap">
          <table className="ranking-table">
            <thead>
              <tr>
                <th>Rank</th>
                <th>Intersection</th>
                <th>Classification</th>
                <th>{renderHeader("compositeScore", "Composite")}</th>
                <th>{renderHeader("aogPct", "AOG %")}</th>
                <th>{renderHeader("aorPct", "AOR %")}</th>
                <th>{renderHeader("avgWait", "Avg Wait (s)")}</th>
                <th>{renderHeader("greenOccupancyPct", "Green Occ %")}</th>
                <th>{renderHeader("redOccupancyPct", "Red Occ %")}</th>
                <th>{renderHeader("splitFailures", "Split Failures")}</th>
                <th>{renderHeader("maxOuts", "Max-Outs")}</th>
                <th>{renderHeader("forceOffs", "Force-Offs")}</th>
              </tr>
            </thead>
            <tbody>
              {sortedRows.map((row) => (
                <tr key={row.id}>
                  <td>{row.rank ?? "—"}</td>
                  <td>
                    <div className="ranking-name-cell">
                      <strong>{row.name}</strong>
                      <span>{row.id} · {row.region}</span>
                    </div>
                  </td>
                  <td>{row.classification}</td>
                  <td className="ranking-moe-cell" style={cellStyle("compositeScore", row.compositeScore)}>
                    {formatValue(row.compositeScore, "", 0)}
                  </td>
                  <td className="ranking-moe-cell" style={cellStyle("aogPct", row.aogPct)}>{formatValue(row.aogPct, "%")}</td>
                  <td className="ranking-moe-cell" style={cellStyle("aorPct", row.aorPct)}>{formatValue(row.aorPct, "%")}</td>
                  <td className="ranking-moe-cell" style={cellStyle("avgWait", row.avgWait)}>{formatValue(row.avgWait, "", 1)}</td>
                  <td className="ranking-moe-cell" style={cellStyle("greenOccupancyPct", row.greenOccupancyPct)}>{formatValue(row.greenOccupancyPct, "%")}</td>
                  <td className="ranking-moe-cell" style={cellStyle("redOccupancyPct", row.redOccupancyPct)}>{formatValue(row.redOccupancyPct, "%")}</td>
                  <td className="ranking-moe-cell" style={cellStyle("splitFailures", row.splitFailures)}>{formatValue(row.splitFailures, "", 0)}</td>
                  <td className="ranking-moe-cell" style={cellStyle("maxOuts", row.maxOuts)}>{formatValue(row.maxOuts, "", 0)}</td>
                  <td className="ranking-moe-cell" style={cellStyle("forceOffs", row.forceOffs)}>{formatValue(row.forceOffs, "", 0)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
}

function DayHourView({
  dayHour,
  loading,
  error,
  selectedSignalId,
  selectedMetric,
  filters,
  setFilters,
  timeOfDayPresets,
  availableDays,
  availableDates,
  maxAvailableDate,
  onSignalChange,
  onMetricChange
}: {
  dayHour: DayHourResponse | null;
  loading: boolean;
  error: string | null;
  selectedSignalId: string;
  selectedMetric: Exclude<RankingMetricKey, "compositeScore">;
  filters: FilterState;
  setFilters: Dispatch<SetStateAction<FilterState>>;
  timeOfDayPresets: Array<{ id: string; label: string; start: string; end: string }>;
  availableDays: string[];
  availableDates: string[];
  maxAvailableDate?: string;
  onSignalChange: (value: string) => void;
  onMetricChange: (value: Exclude<RankingMetricKey, "compositeScore">) => void;
}) {
  const metricOptions: Array<{ key: Exclude<RankingMetricKey, "compositeScore">; label: string }> = [
    { key: "aogPct", label: "AOG %" },
    { key: "aorPct", label: "AOR %" },
    { key: "avgWait", label: "Avg Wait" },
    { key: "greenOccupancyPct", label: "Green Occ %" },
    { key: "redOccupancyPct", label: "Red Occ %" },
    { key: "splitFailures", label: "Split Failures" },
    { key: "maxOuts", label: "Max-Outs" },
    { key: "forceOffs", label: "Force-Offs" }
  ];

  const values = dayHour?.cells.map((cell) => cell.value).filter((value): value is number => value !== null) ?? [];
  const minValue = values.length ? Math.min(...values) : 0;
  const maxValue = values.length ? Math.max(...values) : 1;
  const cellLookup = useMemo(
    () =>
      new Map(
        (dayHour?.cells ?? []).map((cell) => [`${cell.dayName}-${cell.hour}`, cell])
      ),
    [dayHour?.cells]
  );

  const heatColor = (value: number | null) => {
    if (value === null) {
      return "#ffffff";
    }
    const spread = Math.max(maxValue - minValue, 1);
    const ratio = (value - minValue) / spread;
    return `rgba(91, 143, 105, ${0.16 + ratio * 0.7})`;
  };

  return (
    <main className="workspace ranking-workspace">
      {error ? <div className="status-banner error">{error}</div> : null}
      {loading ? <div className="status-banner">Loading day-hour aggregation from the historical SQLite outputs...</div> : null}

      <section className="filters-panel">
        <div className="section-header">
          <div>
            <p className="section-kicker">Filters</p>
            <h2>Day-hour window</h2>
          </div>
          <p className="section-copy">Use the same time-of-day, day-of-week, and date range controls to reshape the matrix for the selected intersection.</p>
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
            <p>Selected signal day-hour matrix</p>
            <strong>
              {selectedSignalId}, {filters.hourFrom} to {filters.hourTo}
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

      <section className="panel historical-hero">
        <div className="historical-top-frame">
          <article className="historical-map-card">
            <div className="section-header compact">
              <div>
                <p className="section-kicker">Map</p>
                <h2>{dayHour?.signal.name ?? "Selected intersection"}</h2>
              </div>
              <p className="section-copy">The map follows the selected historical signal so the day-hour matrix stays tied to one physical location.</p>
            </div>
            <div className="historical-map-frame">
              <MapContainer
                key={`day-hour-map-${selectedSignalId}`}
                center={dayHour?.signal ? [dayHour.signal.lat, dayHour.signal.lon] : [28.54, -81.37]}
                zoom={15}
                scrollWheelZoom={false}
                className="historical-leaflet-map"
              >
                <TileLayer
                  attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                  url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                />
                {dayHour?.signal ? (
                  <Marker position={[dayHour.signal.lat, dayHour.signal.lon]}>
                    <Popup>
                      <strong>{dayHour.signal.name}</strong>
                      <br />
                      Signal ID {dayHour.signal.id}
                    </Popup>
                  </Marker>
                ) : null}
              </MapContainer>
            </div>
          </article>

          <div className="historical-select-wrap">
            <label>
              <span>Intersection</span>
              <select value={selectedSignalId} onChange={(event) => onSignalChange(event.target.value)}>
                {(dayHour?.meta.availableSignals ?? []).map((signal) => (
                  <option key={signal.id} value={signal.id}>
                    {signal.id} · {signal.name}
                  </option>
                ))}
              </select>
            </label>
            <label>
              <span>MOE</span>
              <select value={selectedMetric} onChange={(event) => onMetricChange(event.target.value as Exclude<RankingMetricKey, "compositeScore">)}>
                {metricOptions.map((option) => (
                  <option key={option.key} value={option.key}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <div className="history-stat">
              <span>Metric definition</span>
              <strong>{metricOptions.find((item) => item.key === selectedMetric)?.label ?? selectedMetric}</strong>
              <p className="history-note">{dayHour?.meta.metricDefinitions[selectedMetric] ?? "No definition available."}</p>
            </div>
          </div>
        </div>
      </section>

      <section className="panel">
        <div className="section-header">
          <div>
            <p className="section-kicker">Day-Hour View</p>
            <h2>Month summarized by day-of-week and hour</h2>
          </div>
          <p className="section-copy">Each cell aggregates matching day-hour combinations across the selected date range. Darker cells indicate higher intensity for the chosen MOE.</p>
        </div>
        <div className="table-wrap">
          <table className="day-hour-table">
            <thead>
              <tr>
                <th>Day</th>
                {(dayHour?.hours ?? []).map((hour) => (
                  <th key={hour}>{hour}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {(dayHour?.days ?? []).map((dayName) => (
                <tr key={dayName}>
                  <td className="day-hour-label">{dayName}</td>
                  {(dayHour?.hours ?? []).map((label) => {
                    const hour = Number(label.split(":")[0]);
                    const cell = cellLookup.get(`${dayName}-${hour}`);
                    return (
                      <td
                        key={`${dayName}-${label}`}
                        className="day-hour-cell"
                        style={{ backgroundColor: heatColor(cell?.value ?? null) }}
                        title={
                          cell?.value === null
                            ? `${dayName} ${label}: no data`
                            : `${dayName} ${label}: ${(cell?.value ?? 0).toFixed(1)} across ${cell?.sampleCount ?? 0} matching hourly buckets`
                        }
                      >
                        {cell?.value === null ? "—" : (cell?.value ?? 0).toFixed(1)}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
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
  const [activeView, setActiveView] = useState<"dashboard" | "historical" | "ranking" | "dayHour">("dashboard");
  const [historical, setHistorical] = useState<HistoricalRunResponse | null>(null);
  const [rankingData, setRankingData] = useState<RankingResponse | null>(null);
  const [dayHourData, setDayHourData] = useState<DayHourResponse | null>(null);
  const [historicalSignalId, setHistoricalSignalId] = useState("1470");
  const [dayHourMetric, setDayHourMetric] = useState<Exclude<RankingMetricKey, "compositeScore">>("avgWait");
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
  const [rankingLoading, setRankingLoading] = useState(false);
  const [dayHourLoading, setDayHourLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [historicalError, setHistoricalError] = useState<string | null>(null);
  const [rankingError, setRankingError] = useState<string | null>(null);
  const [dayHourError, setDayHourError] = useState<string | null>(null);

  const buildDashboardParams = (intersectionIdsOverride?: string[]) =>
    new URLSearchParams({
      dateFrom: filters.dateFrom,
      dateTo: filters.dateTo,
      daysOfWeek: filters.daysOfWeek.join(","),
      hourFrom: filters.hourFrom,
      hourTo: filters.hourTo,
      timeOfDayPreset: filters.timeOfDayPreset,
      phase: filters.selectedPhase,
      intersectionIds: (intersectionIdsOverride ?? selectedIntersectionIds).join(",")
    });

  useEffect(() => {
    const controller = new AbortController();

    async function loadDashboard() {
      setLoading(true);
      setError(null);
      const params = buildDashboardParams();

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
    if (activeView !== "ranking") {
      return;
    }
    const controller = new AbortController();

    async function loadRankingDashboard() {
      setRankingLoading(true);
      setRankingError(null);
      try {
        const params = new URLSearchParams({
          dateFrom: filters.dateFrom,
          dateTo: filters.dateTo,
          daysOfWeek: filters.daysOfWeek.join(","),
          hourFrom: filters.hourFrom,
          hourTo: filters.hourTo,
          timeOfDayPreset: filters.timeOfDayPreset
        });
        const response = await fetch(`http://127.0.0.1:8000/api/ranking?${params.toString()}`, {
          signal: controller.signal
        });
        if (!response.ok) {
          throw new Error(`Backend request failed with status ${response.status}`);
        }
        const nextRanking: RankingResponse = await response.json();
        setRankingData(nextRanking);
      } catch (requestError) {
        if ((requestError as Error).name !== "AbortError") {
          setRankingError((requestError as Error).message);
        }
      } finally {
        setRankingLoading(false);
      }
    }

    loadRankingDashboard();
    return () => controller.abort();
  }, [
    activeView,
    filters.dateFrom,
    filters.dateTo,
    filters.daysOfWeek.join(","),
    filters.hourFrom,
    filters.hourTo,
    filters.timeOfDayPreset,
    filters.selectedPhase
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
        const params = new URLSearchParams({ signalId: historicalSignalId });
        const response = await fetch(`http://127.0.0.1:8000/api/historical-run?${params.toString()}`, {
          signal: controller.signal
        });
        if (!response.ok) {
          throw new Error(`Backend request failed with status ${response.status}`);
        }
        const nextHistorical: HistoricalRunResponse = await response.json();
        setHistorical(nextHistorical);
        if (nextHistorical.selectedSignalId) {
          setHistoricalSignalId(nextHistorical.selectedSignalId);
        }
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
  }, [activeView, historicalSignalId]);

  useEffect(() => {
    if (activeView !== "dayHour") {
      return;
    }
    const controller = new AbortController();

    async function loadDayHourView() {
      setDayHourLoading(true);
      setDayHourError(null);
      try {
        const params = new URLSearchParams({
          signalId: historicalSignalId,
          metric: dayHourMetric,
          dateFrom: filters.dateFrom,
          dateTo: filters.dateTo,
          daysOfWeek: filters.daysOfWeek.join(","),
          hourFrom: filters.hourFrom,
          hourTo: filters.hourTo,
          timeOfDayPreset: filters.timeOfDayPreset
        });
        const response = await fetch(`http://127.0.0.1:8000/api/day-hour?${params.toString()}`, {
          signal: controller.signal
        });
        if (!response.ok) {
          throw new Error(`Backend request failed with status ${response.status}`);
        }
        const nextDayHour: DayHourResponse = await response.json();
        setDayHourData(nextDayHour);
      } catch (requestError) {
        if ((requestError as Error).name !== "AbortError") {
          setDayHourError((requestError as Error).message);
        }
      } finally {
        setDayHourLoading(false);
      }
    }

    loadDayHourView();
    return () => controller.abort();
  }, [
    activeView,
    historicalSignalId,
    dayHourMetric,
    filters.dateFrom,
    filters.dateTo,
    filters.daysOfWeek.join(","),
    filters.hourFrom,
    filters.hourTo,
    filters.timeOfDayPreset
  ]);

  const corridor = dashboard?.corridor;
  const intersections = corridor?.intersections ?? [];
  const allIntersections = corridor?.allIntersections ?? [];
  const selectedIntersection = intersections[0];

  const toggleIntersectionSelection = (intersectionId: string) => {
    setSelectedIntersectionIds((current) => {
      const active = current.includes(intersectionId);
      if (active && current.length === 1) {
        return current;
      }
      return active ? current.filter((id) => id !== intersectionId) : [...current, intersectionId];
    });
  };

  const rankedIntersections = useMemo<RankedIntersection[]>(() => {
    const intersectionsWithData = allIntersections.filter(
      (intersection) => intersection.phases.length > 0 || intersection.trend.length > 0
    );

    const maxDelay = Math.max(1, ...intersectionsWithData.map((item) => item.summary.meanDelay));
    const maxSplitFailures = Math.max(1, ...intersectionsWithData.map((item) => item.summary.splitFailures));
    const maxMaxOuts = Math.max(1, ...intersectionsWithData.map((item) => item.summary.maxOutCount));

    const scored = intersectionsWithData
      .map((intersection) => {
        const aogComponent = intersection.summary.meanAog / 100;
        const delayComponent = 1 - intersection.summary.meanDelay / maxDelay;
        const splitComponent = 1 - intersection.summary.splitFailures / maxSplitFailures;
        const maxOutComponent = 1 - intersection.summary.maxOutCount / maxMaxOuts;
        const score =
          (aogComponent * 0.4 + delayComponent * 0.3 + splitComponent * 0.15 + maxOutComponent * 0.15) * 100;

        return { ...intersection, score };
      })
      .sort((left, right) => right.score - left.score);

    const bestCutoff = Math.max(1, Math.ceil(scored.length / 3));
    const watchCutoff = Math.max(bestCutoff + 1, Math.ceil((scored.length * 2) / 3));

    const ranked = scored.map((intersection, index) => {
      const category: RankedIntersection["category"] =
        index < bestCutoff ? "best" : index < watchCutoff ? "watch" : "alert";
      return {
        ...intersection,
        rank: index + 1,
        score: intersection.score,
        category
      };
    });

    const noData = allIntersections
      .filter((intersection) => !intersectionsWithData.some((candidate) => candidate.id === intersection.id))
      .map((intersection) => ({
        ...intersection,
        rank: null,
        score: null,
        category: "nodata" as const
      }));

    return [...ranked, ...noData];
  }, [allIntersections]);

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
        <button
          type="button"
          className={activeView === "ranking" ? "active" : ""}
          onClick={() => setActiveView("ranking")}
        >
          Ranking
        </button>
        <button
          type="button"
          className={activeView === "dayHour" ? "active" : ""}
          onClick={() => setActiveView("dayHour")}
        >
          Day-Hour View
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
              {rankedIntersections.map((intersection) => {
                const active = selectedIntersectionIds.includes(intersection.id);
                return (
                  <Fragment key={intersection.id}>
                    <Marker
                      position={[intersection.lat, intersection.lon]}
                      icon={buildMarkerIcon(intersection.category, active)}
                      eventHandlers={{ click: () => toggleIntersectionSelection(intersection.id) }}
                    >
                      <Popup>
                        <strong>{intersection.name}</strong>
                        <br />
                        Signal ID {intersection.id}
                        <br />
                        {intersection.rank !== null ? `Rank #${intersection.rank}` : "No ranked data"}
                        {intersection.score !== null ? ` · Score ${intersection.score.toFixed(0)}` : ""}
                      </Popup>
                    </Marker>
                    {active ? (
                      <CircleMarker
                        center={[intersection.lat, intersection.lon]}
                        radius={18}
                        pathOptions={{ color: "#68a77e", opacity: 0.65, weight: 3, fillOpacity: 0.08 }}
                      />
                    ) : null}
                  </Fragment>
                );
              })}
            </MapContainer>

            <aside className="corridor-list">
              <p className="section-kicker">Intersections</p>
              <p className="list-note">
                {loading
                  ? "Loading mapped signals from the October ATSPM files..."
                  : `${rankedIntersections.length} mapped signals are ranked from the current filtered window.`}
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
              {rankedIntersections.map((intersection) => {
                const active = selectedIntersectionIds.includes(intersection.id);
                return (
                  <label
                    key={intersection.id}
                    className={`intersection-row tone-${intersection.category} ${active ? "active" : ""}`}
                  >
                    <input
                      type="checkbox"
                      checked={active}
                      onChange={() => toggleIntersectionSelection(intersection.id)}
                    />
                    <span className="row-index">{intersection.rank ? `#${String(intersection.rank).padStart(2, "0")}` : "--"}</span>
                    <div className="intersection-copy">
                      <span className="intersection-name">{intersection.name}</span>
                      <span className="intersection-meta">
                        {intersection.rank !== null ? `Rank #${intersection.rank}` : "No ranked data"}
                        {intersection.score !== null ? ` · Score ${intersection.score.toFixed(0)}` : ""}
                        {intersection.score !== null
                          ? ` · AOG ${intersection.summary.meanAog.toFixed(1)}% · Wait ${intersection.summary.meanDelay.toFixed(1)} s`
                          : " · No aggregated ATSPM data in the current filter"}
                      </span>
                    </div>
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
      ) : activeView === "historical" ? (
        <HistoricalRunView
          historical={historical}
          loading={historicalLoading}
          error={historicalError}
          historicalSignalId={historicalSignalId}
          onSignalChange={setHistoricalSignalId}
        />
      ) : activeView === "ranking" ? (
        <main className="workspace ranking-workspace">
          <section className="filters-panel">
            <div className="section-header">
              <div>
                <p className="section-kicker">Filters</p>
                <h2>Ranking window</h2>
              </div>
              <p className="section-copy">Use the same day and hour filters here to classify all intersections from the historical SQLite outputs.</p>
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
                <p>Historical SQLite ranking</p>
                <strong>
                  {rankingData?.meta.signalsWithData ?? 0} signals with data, {filters.hourFrom} to {filters.hourTo}
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

          <RankingView
            ranking={rankingData}
            loading={rankingLoading}
            error={rankingError}
          />
        </main>
      ) : (
        <DayHourView
          dayHour={dayHourData}
          loading={dayHourLoading}
          error={dayHourError}
          selectedSignalId={historicalSignalId}
          selectedMetric={dayHourMetric}
          filters={filters}
          setFilters={setFilters}
          timeOfDayPresets={timeOfDayPresets}
          availableDays={availableDays}
          availableDates={availableDates}
          maxAvailableDate={maxAvailableDate}
          onSignalChange={setHistoricalSignalId}
          onMetricChange={setDayHourMetric}
        />
      )}
    </div>
  );
}
