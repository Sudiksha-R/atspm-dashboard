import { Fragment, useEffect, useMemo, useRef, useState, type ComponentType, type Dispatch, type SetStateAction } from "react";
import { CircleMarker, MapContainer, Marker, Polyline, Popup, TileLayer, useMap } from "react-leaflet";
import { divIcon, LatLngExpression } from "leaflet";
import createPlotlyComponent from "react-plotly.js/factory";
import Plotly from "plotly.js-dist-min";
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

const Plot = createPlotlyComponent(Plotly as never) as ComponentType<Record<string, unknown>>;
const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL as string | undefined)?.replace(/\/$/, "") ?? "http://127.0.0.1:8000";

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

type BottleneckSlot = {
  dayName: string;
  hour: number;
  label: string;
  sampleCount: number;
  clusterId: number | null;
  clusterLabel: string;
  isOutlier: boolean;
  aogPct: number | null;
  aorPct: number | null;
  volume: number | null;
  avgWait: number | null;
  greenOccupancyPct: number | null;
  redOccupancyPct: number | null;
  splitFailures: number | null;
  maxOuts: number | null;
  forceOffs: number | null;
};

type BottleneckResponse = {
  meta: {
    dataSource: string;
    availableSignals: Array<{
      id: string;
      name: string;
      route: string;
      region: string;
      lat: number;
      lon: number;
    }>;
    moeOptions: Array<{
      key: string;
      label: string;
      description: string;
    }>;
    algorithm: {
      name: string;
      eps: number;
      minSamples: number;
      normalization?: string;
      normalizationStats?: Record<string, { p05: number; p95: number; range: number }>;
    };
  };
  filters: {
    signalId: string;
    phase: number;
    dateFrom: string;
    dateTo: string;
    daysOfWeek: string[];
    hourFrom: string;
    hourTo: string;
    selectedMoes: string[];
    usedMoes: string[];
    eps: number;
    minSamples: number;
  };
  signal: {
    id: string;
    name: string;
    route: string;
    region: string;
    lat: number;
    lon: number;
  };
  availablePhases: number[];
  hours: string[];
  days: string[];
  summary: {
    slotCount: number;
    slotsWithData: number;
    clusterCount: number;
    outlierCount: number;
    changeCount: number;
  };
  slots: BottleneckSlot[];
  clusterSummaries: Array<{
    clusterId: number;
    clusterLabel: string;
    slotCount: number;
    slots: string[];
    slotsPreview: string[];
    timeBand: string | null;
    dayPattern: string | null;
    performanceTag: string | null;
    signature: string | null;
    summarySentence: string | null;
    [key: string]: string | number | string[] | null;
  }>;
  changeDetections: Array<{
    dayName: string;
    fromHour: number;
    fromLabel: string;
    toHour: number;
    toLabel: string;
    fromClusterLabel: string;
    toClusterLabel: string;
    transitionType: string;
    severityScore: number;
    severityLabel: string;
    summarySentence: string;
    topChanges: string[];
  }>;
};

type HdbscanResponse = {
  meta: {
    dataSource: string;
    availableSignals: Array<{
      id: string;
      name: string;
      route: string;
      region: string;
      lat: number;
      lon: number;
    }>;
    selectionPresets: Array<{
      id: string;
      label: string;
      intersectionIds: string[];
    }>;
    moeOptions: Array<{
      key: string;
      label: string;
      description: string;
    }>;
    algorithm: {
      name: string;
      minClusterSize: number;
      minSamples: number;
      clusterSelectionEpsilon: number;
      normalization?: string;
      normalizationStats?: Record<string, { p05: number; p95: number; range: number }>;
    };
  };
  filters: {
    signalIds: string[];
    phases: number[];
    dateFrom: string;
    dateTo: string;
    daysOfWeek: string[];
    hourFrom: string;
    hourTo: string;
    selectedMoes: string[];
    usedMoes: string[];
    minClusterSize: number;
    minSamples: number;
    clusterSelectionEpsilon: number;
  };
  selectedSignals: Array<{
    id: string;
    name: string;
    route: string;
    region: string;
    lat: number;
    lon: number;
  }>;
  availablePhases: number[];
  phaseAvailability: Record<string, number[]>;
  hours: string[];
  days: string[];
  summary: {
    selectedSignalCount: number;
    selectedPhaseCount: number;
    slotCount: number;
    slotsWithData: number;
    clusterCount: number;
    outlierCount: number;
    changeCount: number;
  };
  slots: BottleneckSlot[];
  clusterSummaries: BottleneckResponse["clusterSummaries"];
  changeDetections: BottleneckResponse["changeDetections"];
};

type ChangeDetectionRow = {
  signalId: string;
  signalName: string;
  date: string;
  dayName: string;
  hour: number | null;
  label: string;
  value: number | null;
  historicalMean: number | null;
  historyCount: number;
  deltaPct: number | null;
  status: string;
  direction: string;
  tone: "calm" | "watch" | "alert";
};

type ChangeDetectionResponse = {
  meta: {
    dataSource: string;
    availableSignals: Array<{
      id: string;
      name: string;
      route: string;
      region: string;
      lat: number;
      lon: number;
    }>;
    selectionPresets: Array<{
      id: string;
      label: string;
      intersectionIds: string[];
    }>;
    moeOptions: Array<{
      key: Exclude<RankingMetricKey, "compositeScore">;
      label: string;
      description: string;
    }>;
    granularityOptions: Array<{
      key: "day" | "hour";
      label: string;
    }>;
  };
  filters: {
    signalIds: string[];
    moe: Exclude<RankingMetricKey, "compositeScore">;
    granularity: "day" | "hour";
    dateFrom: string;
    dateTo: string;
    hourFrom: string;
    hourTo: string;
    selectedDate: string;
    selectedHour: string;
  };
  slotSelection: {
    date: string;
    dayName: string | null;
    hour: string;
  };
  summary: {
    selectedSignalCount: number;
    pointCount: number;
    baselineReady: number;
    changeCount: number;
    stableCount: number;
    pendingCount: number;
    slotPointCount: number;
    slotChangeCount: number;
  };
  intersectionSummaries: Array<{
    signalId: string;
    signalName: string;
    currentValue: number | null;
    historicalMean: number | null;
    deltaPct: number | null;
    status: string;
    tone: "calm" | "watch" | "alert";
    historyCount: number;
  }>;
  mapStatuses: Array<{
    signalId: string;
    signalName: string;
    lat: number;
    lon: number;
    deltaPct: number | null;
    tone: "calm" | "watch" | "alert";
  }>;
  availableDates: string[];
  availableHours: string[];
  rows: ChangeDetectionRow[];
  slotRows: ChangeDetectionRow[];
  topChanges: ChangeDetectionRow[];
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

type CompositeWeights = {
  aogPct: number;
  aorPct: number;
  avgWait: number;
  splitFailures: number;
  maxOuts: number;
  forceOffs: number;
};

function buildMarkerIcon(tone: RankedIntersection["category"], active: boolean) {
  return divIcon({
    className: "intersection-pin-shell",
    html: `<span class="map-pin map-pin--${tone}${active ? " map-pin--active" : ""}"></span>`,
    iconSize: [24, 24],
    iconAnchor: [12, 12]
  });
}

function interpolateColor(start: [number, number, number], end: [number, number, number], ratio: number) {
  return start.map((value, index) => Math.round(value + (end[index] - value) * ratio)) as [number, number, number];
}

function buildRankMarkerIcon(rank: number | null, total: number, active: boolean) {
  if (rank === null || total <= 0) {
    return buildMarkerIcon("nodata", active);
  }

  const ratio = total <= 1 ? 0 : (rank - 1) / (total - 1);
  const light = interpolateColor([245, 223, 242], [220, 236, 255], ratio);
  const mid = interpolateColor([221, 119, 186], [100, 149, 237], ratio);
  const dark = interpolateColor([164, 60, 132], [52, 91, 166], ratio);
  const gradient = `radial-gradient(circle at 30% 30%, rgb(${light.join(",")}), rgb(${mid.join(",")}) 48%, rgb(${dark.join(",")}) 100%)`;

  return divIcon({
    className: "intersection-pin-shell",
    html: `<span class="map-pin${active ? " map-pin--active" : ""}" style="background:${gradient}"></span>`,
    iconSize: [24, 24],
    iconAnchor: [12, 12]
  });
}

function rankColorParts(rank: number | null, total: number) {
  if (rank === null || total <= 0) {
      return {
      light: [244, 240, 247] as [number, number, number],
      mid: [198, 188, 214] as [number, number, number],
      dark: [128, 119, 148] as [number, number, number]
    };
  }

  const ratio = total <= 1 ? 0 : (rank - 1) / (total - 1);
  return {
    light: interpolateColor([245, 223, 242], [220, 236, 255], ratio),
    mid: interpolateColor([221, 119, 186], [100, 149, 237], ratio),
    dark: interpolateColor([164, 60, 132], [52, 91, 166], ratio)
  };
}

function buildChangeMarkerIcon(tone: "calm" | "watch" | "alert", active: boolean) {
  const styles = {
    calm: {
      light: "rgb(223, 238, 255)",
      mid: "rgb(120, 167, 235)",
      dark: "rgb(55, 97, 176)",
    },
    watch: {
      light: "rgb(255, 245, 199)",
      mid: "rgb(238, 204, 92)",
      dark: "rgb(187, 145, 28)",
    },
    alert: {
      light: "rgb(255, 224, 185)",
      mid: "rgb(241, 149, 64)",
      dark: "rgb(199, 103, 28)",
    },
  }[tone];
  const gradient = `radial-gradient(circle at 30% 30%, ${styles.light}, ${styles.mid} 48%, ${styles.dark} 100%)`;
  return divIcon({
    className: "intersection-pin-shell",
    html: `<span class="map-pin${active ? " map-pin--active" : ""}" style="background:${gradient}"></span>`,
    iconSize: [24, 24],
    iconAnchor: [12, 12]
  });
}

function RankingMapFocus({ center, zoom }: { center: LatLngExpression; zoom: number }) {
  const map = useMap();

  useEffect(() => {
    map.setView(center, zoom, { animate: true });
  }, [center, map, zoom]);

  return null;
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

function CompositeScoreInfo({
  shortDescription,
  onOpen
}: {
  shortDescription: string;
  onOpen: () => void;
}) {
  return (
    <span className="composite-info-group">
      <MetricInfo description={shortDescription} />
      <button type="button" className="composite-info-button" onClick={onOpen} aria-label="Open composite score explanation">
        <span className="composite-info-button-icon">
          <UiIcon kind="info" />
        </span>
      </button>
    </span>
  );
}

function CompositeWeightEditor({
  weights,
  onChange
}: {
  weights: CompositeWeights;
  onChange: (key: keyof CompositeWeights, value: number) => void;
}) {
  const total = Math.max(1, Object.values(weights).reduce((sum, value) => sum + value, 0));
  const normalizedWeights = {
    aogPct: (weights.aogPct / total) * 100,
    aorPct: (weights.aorPct / total) * 100,
    avgWait: (weights.avgWait / total) * 100,
    splitFailures: (weights.splitFailures / total) * 100,
    maxOuts: (weights.maxOuts / total) * 100,
    forceOffs: (weights.forceOffs / total) * 100
  };
  const latexMarkup = `
    <math display="block" xmlns="http://www.w3.org/1998/Math/MathML">
      <mrow>
        <mtext>Composite Score</mtext>
        <mo>=</mo>
        <mn>100</mn>
        <mo>&#x00D7;</mo>
        <mo>[</mo>
        <mfrac><mn>${normalizedWeights.aogPct.toFixed(1)}</mn><mn>100</mn></mfrac>
        <mo>(</mo><mfrac><mtext>AOG</mtext><mn>100</mn></mfrac><mo>)</mo>
        <mo>+</mo>
        <mfrac><mn>${normalizedWeights.aorPct.toFixed(1)}</mn><mn>100</mn></mfrac>
        <mo>(</mo><mn>1</mn><mo>-</mo><mfrac><mtext>AOR</mtext><mn>100</mn></mfrac><mo>)</mo>
        <mo>+</mo>
        <mfrac><mn>${normalizedWeights.avgWait.toFixed(1)}</mn><mn>100</mn></mfrac>
        <mo>(</mo><mn>1</mn><mo>-</mo><mfrac><mtext>AvgWait</mtext><mtext>maxAvgWait</mtext></mfrac><mo>)</mo>
        <mo>+</mo>
        <mfrac><mn>${normalizedWeights.splitFailures.toFixed(1)}</mn><mn>100</mn></mfrac>
        <mo>(</mo><mn>1</mn><mo>-</mo><mfrac><mtext>SplitFailures</mtext><mtext>maxSplitFailures</mtext></mfrac><mo>)</mo>
        <mo>+</mo>
        <mfrac><mn>${normalizedWeights.maxOuts.toFixed(1)}</mn><mn>100</mn></mfrac>
        <mo>(</mo><mn>1</mn><mo>-</mo><mfrac><mtext>MaxOuts</mtext><mtext>maxMaxOuts</mtext></mfrac><mo>)</mo>
        <mo>+</mo>
        <mfrac><mn>${normalizedWeights.forceOffs.toFixed(1)}</mn><mn>100</mn></mfrac>
        <mo>(</mo><mn>1</mn><mo>-</mo><mfrac><mtext>ForceOffs</mtext><mtext>maxForceOffs</mtext></mfrac><mo>)</mo>
        <mo>]</mo>
      </mrow>
    </math>
  `;

  return (
    <section className="panel composite-editor-panel">
      <div className="section-header">
        <div>
          <p className="section-kicker">Composite Score</p>
          <h2>Weight editor and formula</h2>
        </div>
        <p className="section-copy">
          This score is a custom relative ranking index for the current filtered window. Enter any raw weights you want below; the dashboard automatically converts them into normalized percentages that always sum to 100%.
        </p>
      </div>

      <div className="composite-editor-grid">
        {[
          ["aogPct", "AOG"],
          ["aorPct", "AOR"],
          ["avgWait", "Avg Wait"],
          ["splitFailures", "Split Failures"],
          ["maxOuts", "Max-Outs"],
          ["forceOffs", "Force-Offs"]
        ].map(([key, label]) => (
          <label key={key} className="composite-weight-card">
            <span>{label}</span>
            <input
              type="number"
              min="0"
              step="1"
              value={weights[key as keyof CompositeWeights]}
              onChange={(event) => onChange(key as keyof CompositeWeights, Number(event.target.value))}
            />
            <p className="composite-weight-meta">Raw weight</p>
            <strong>{normalizedWeights[key as keyof CompositeWeights].toFixed(1)}%</strong>
          </label>
        ))}
      </div>

      <div className="composite-editor-footer">
        <div className="modal-formula composite-formula-card">
          <p className="modal-formula-label">Normalized formula used in ranking</p>
          <div className="latex-formula" dangerouslySetInnerHTML={{ __html: latexMarkup }} />
        </div>
      </div>
    </section>
  );
}

function CustomSelect({
  value,
  options,
  onChange,
  className = "",
  panelClassName = ""
}: {
  value: string;
  options: Array<{ value: string; label: string }>;
  onChange: (value: string) => void;
  className?: string;
  panelClassName?: string;
}) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const handlePointerDown = (event: MouseEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) {
        setOpen(false);
      }
    };

    document.addEventListener("mousedown", handlePointerDown);
    return () => document.removeEventListener("mousedown", handlePointerDown);
  }, []);

  const selected = options.find((option) => option.value === value) ?? options[0];

  return (
    <div ref={rootRef} className={`custom-select ${className} ${open ? "is-open" : ""}`.trim()}>
      <button
        type="button"
        className="custom-select-trigger"
        onClick={() => setOpen((current) => !current)}
        aria-haspopup="listbox"
        aria-expanded={open}
      >
        <span>{selected?.label ?? value}</span>
        <span className="custom-select-caret">▾</span>
      </button>
      {open ? (
        <div className={`custom-select-panel ${panelClassName}`.trim()} role="listbox">
          {options.map((option) => {
            const active = option.value === value;
            return (
              <button
                key={option.value}
                type="button"
                className={`custom-select-option ${active ? "is-active" : ""}`}
                onClick={() => {
                  onChange(option.value);
                  setOpen(false);
                }}
              >
                <span className="custom-select-check">{active ? "✓" : ""}</span>
                <span>{option.label}</span>
              </button>
            );
          })}
        </div>
      ) : null}
    </div>
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
        <div className="status-banner">Loading the saved run summary for signal {historicalSignalId}...</div>
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
              <CustomSelect
                value={historicalSignalId}
                options={(historical?.availableSignals ?? []).map((signal) => ({
                  value: signal.id,
                  label: `${signal.id} · ${signal.name}`
                }))}
                onChange={onSignalChange}
                panelClassName="custom-select-panel--signals"
              />
            </label>
          </div>
        </div>

        {!activeHistorical?.exists ? (
          <div className="empty-chart-state">
            <strong>Saved run not generated yet.</strong>
            <p>{activeHistorical?.message ?? "This signal does not have a saved summary yet."}</p>
          </div>
        ) : activeHistorical.exists && days.length === 0 ? (
          <div className="empty-chart-state">
            <strong>No saved rows were found for this signal.</strong>
            <p>The saved output exists, but the current source files did not produce day-level results for this intersection.</p>
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
              <p className="section-kicker">Saved Output</p>
                <h2>Daily run status</h2>
              </div>
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
  error,
  intersections
}: {
  ranking: RankingResponse | null;
  loading: boolean;
  error: string | null;
  intersections: IntersectionPayload[];
}) {
  const [sortMetric, setSortMetric] = useState<RankingMetricKey>("compositeScore");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc");
  const [selectedRankingIntersectionId, setSelectedRankingIntersectionId] = useState<string | null>(null);
  const [weights, setWeights] = useState<CompositeWeights>({
    aogPct: 30,
    aorPct: 10,
    avgWait: 20,
    splitFailures: 15,
    maxOuts: 15,
    forceOffs: 10
  });
  const totalWeight = Math.max(1, Object.values(weights).reduce((sum, value) => sum + value, 0));
  const dataRows = useMemo(() => {
    const usable = (ranking?.rows ?? []).filter((row) => row.hasData);
    const maxWait = Math.max(1, ...usable.map((row) => row.avgWait ?? 0));
    const maxSplit = Math.max(1, ...usable.map((row) => row.splitFailures ?? 0));
    const maxMaxOuts = Math.max(1, ...usable.map((row) => row.maxOuts ?? 0));
    const maxForceOffs = Math.max(1, ...usable.map((row) => row.forceOffs ?? 0));

    return usable
      .map((row) => {
        const compositeScore =
          ((row.aogPct ?? 0) / 100) * (weights.aogPct / totalWeight) +
          (1 - (row.aorPct ?? 100) / 100) * (weights.aorPct / totalWeight) +
          (1 - (row.avgWait ?? maxWait) / maxWait) * (weights.avgWait / totalWeight) +
          (1 - (row.splitFailures ?? maxSplit) / maxSplit) * (weights.splitFailures / totalWeight) +
          (1 - (row.maxOuts ?? maxMaxOuts) / maxMaxOuts) * (weights.maxOuts / totalWeight) +
          (1 - (row.forceOffs ?? maxForceOffs) / maxForceOffs) * (weights.forceOffs / totalWeight);

        return { ...row, compositeScore: Math.max(0, Math.min(100, compositeScore * 100)) };
      })
      .sort((left, right) => right.compositeScore - left.compositeScore);
  }, [ranking?.rows, totalWeight, weights]);

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

  const compositeExplanation =
    "Custom 0 to 100 relative score built from AOG, AOR, average wait, split failures, max-outs, and force-offs for the current filtered window. The weights are editable in the composite score panel.";

  const updateWeight = (key: keyof CompositeWeights, value: number) => {
    setWeights((current) => ({
      ...current,
      [key]: Number.isFinite(value) ? Math.max(0, Math.round(value)) : 0
    }));
  };

  const rankingIntersectionMeta = useMemo(() => {
    const byId = new Map<string, IntersectionPayload>();
    for (const source of intersections) {
      if (!byId.has(source.id)) {
        byId.set(source.id, source);
      }
    }
    return byId;
  }, [intersections]);

  const rankingMapRows = useMemo(() => {
    return rankedRows
      .map((row) => {
        const meta = rankingIntersectionMeta.get(row.id);
        if (!meta) {
          return null;
        }
        const category: RankedIntersection["category"] =
          row.classification === "Top Quartile"
            ? "best"
            : row.classification === "Upper-Mid" || row.classification === "Lower-Mid"
              ? "watch"
              : row.classification === "Bottom Quartile"
                ? "alert"
                : "nodata";
        return {
          ...row,
          lat: meta.lat,
          lon: meta.lon,
          category
        };
      })
      .filter((row): row is NonNullable<typeof row> => row !== null);
  }, [rankedRows, rankingIntersectionMeta]);

  const rankingMapPath = useMemo(
    () =>
      [...rankingMapRows]
        .sort((left, right) => Number(left.id) - Number(right.id))
        .map((row) => [row.lat, row.lon] as LatLngExpression),
    [rankingMapRows]
  );
  const rankedMarkerCount = useMemo(
    () => rankingMapRows.filter((row) => row.rank !== null).length,
    [rankingMapRows]
  );
  const rankingListRows = useMemo(
    () =>
      [...rankingMapRows].sort((left, right) => {
        const leftRank = left.rank ?? Number.POSITIVE_INFINITY;
        const rightRank = right.rank ?? Number.POSITIVE_INFINITY;
        if (leftRank === rightRank) {
          return Number(left.id) - Number(right.id);
        }
        return leftRank - rightRank;
      }),
    [rankingMapRows]
  );

  useEffect(() => {
    if (!rankingListRows.length) {
      setSelectedRankingIntersectionId(null);
      return;
    }
    if (!selectedRankingIntersectionId || !rankingListRows.some((row) => row.id === selectedRankingIntersectionId)) {
      setSelectedRankingIntersectionId(rankingListRows[0].id);
    }
  }, [rankingListRows, selectedRankingIntersectionId]);

  const selectedRankingRow = useMemo(
    () => rankingListRows.find((row) => row.id === selectedRankingIntersectionId) ?? null,
    [rankingListRows, selectedRankingIntersectionId]
  );

  const rankingMapCenter = useMemo<LatLngExpression>(() => {
    if (selectedRankingRow) {
      return [selectedRankingRow.lat, selectedRankingRow.lon];
    }
    if (!rankingMapRows.length) {
      return [27.2989, -82.48];
    }
    const lat = rankingMapRows.reduce((sum, row) => sum + row.lat, 0) / rankingMapRows.length;
    const lon = rankingMapRows.reduce((sum, row) => sum + row.lon, 0) / rankingMapRows.length;
    return [lat, lon];
  }, [rankingMapRows, selectedRankingRow]);

  const rankingMapZoom = selectedRankingRow ? 16 : 12;

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
      {loading ? <div className="status-banner">Loading precomputed ranking metrics across all signals...</div> : null}

      <CompositeWeightEditor weights={weights} onChange={updateWeight} />

      <section className="panel">
        <div className="section-header">
          <div>
            <p className="section-kicker">Ranking</p>
            <div className="ranking-title-row">
              <h2>Intersection classification matrix</h2>
            </div>
          </div>
        </div>

        <div className="historical-summary-grid bottleneck-summary-grid">
          <article className="history-stat">
            <span>Mapped signals</span>
            <strong>{ranking?.meta.availableSignals ?? 0}</strong>
          </article>
          <article className="history-stat">
            <span>Built summaries</span>
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

      <section className="panel ranking-map-panel">
        <div className="section-header">
          <div>
            <div className="ranking-title-row">
              <h2>Composite score by intersection</h2>
            </div>
          </div>
        </div>
        <div className="ranking-map-layout">
          <aside className="ranking-rank-list">
            <p className="section-kicker">Ranks</p>
            <div className="ranking-rank-list-inner">
              {rankingListRows.map((row) => (
                <button
                  key={`ranking-list-${row.id}`}
                  type="button"
                  className={`ranking-rank-row ${row.id === selectedRankingIntersectionId ? "selected" : ""}`}
                  onClick={() => setSelectedRankingIntersectionId(row.id)}
                  style={{
                    background: (() => {
                      const { light, mid } = rankColorParts(row.rank, rankedMarkerCount);
                      return `linear-gradient(135deg, rgba(${light.join(",")}, 0.22) 0%, rgba(${mid.join(",")}, 0.12) 100%)`;
                    })(),
                    borderColor: (() => {
                      const { mid } = rankColorParts(row.rank, rankedMarkerCount);
                      return `rgba(${mid.join(",")}, 0.45)`;
                    })()
                  }}
                >
                  <span
                    className="ranking-rank-badge"
                    style={{
                      borderColor: (() => {
                        const { mid } = rankColorParts(row.rank, rankedMarkerCount);
                        return `rgba(${mid.join(",")}, 0.7)`;
                      })(),
                      color: (() => {
                        const { dark } = rankColorParts(row.rank, rankedMarkerCount);
                        return `rgb(${dark.join(",")})`;
                      })()
                    }}
                  >
                    {row.rank !== null ? `#${row.rank}` : "—"}
                  </span>
                  <div className="ranking-rank-copy">
                    <strong
                      style={{
                        color: (() => {
                          const { dark } = rankColorParts(row.rank, rankedMarkerCount);
                          return `rgb(${dark.join(",")})`;
                        })()
                      }}
                    >
                      {row.id}: {row.name}
                    </strong>
                    <span>{row.compositeScore !== null ? `${row.compositeScore.toFixed(0)} composite` : row.classification}</span>
                  </div>
                </button>
              ))}
            </div>
          </aside>

          <div className="ranking-map-column">
            <div className="ranking-map-frame">
              <MapContainer
                center={rankingMapCenter}
                zoom={rankingMapZoom}
                scrollWheelZoom={false}
                className="leaflet-map ranking-leaflet-map"
              >
                <RankingMapFocus center={rankingMapCenter} zoom={rankingMapZoom} />
                <TileLayer
                  attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                  url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                />
                {rankingMapPath.length > 1 ? (
                  <Polyline positions={rankingMapPath} pathOptions={{ color: "#c98358", weight: 5, opacity: 0.7 }} />
                ) : null}
                {rankingMapRows.map((row) => (
                  <Fragment key={`ranking-map-${row.id}`}>
                    <Marker
                      position={[row.lat, row.lon]}
                      icon={buildRankMarkerIcon(row.rank, rankedMarkerCount, row.id === selectedRankingIntersectionId)}
                      eventHandlers={{
                        click: () => setSelectedRankingIntersectionId(row.id)
                      }}
                    >
                      <Popup>
                        <strong>{row.name}</strong>
                        <br />
                        Signal ID {row.id}
                        <br />
                        {row.rank !== null ? `Rank #${row.rank}` : "No ranked data"}
                        <br />
                        {row.compositeScore !== null ? `Composite ${row.compositeScore.toFixed(0)}` : "No composite score"}
                        <br />
                        {row.classification}
                      </Popup>
                    </Marker>
                    {row.id === selectedRankingIntersectionId ? (
                      <CircleMarker
                        center={[row.lat, row.lon]}
                        radius={20}
                        pathOptions={{ color: "#68a77e", opacity: 0.34, weight: 2, fillOpacity: 0.06 }}
                      />
                    ) : null}
                  </Fragment>
                ))}
              </MapContainer>
            </div>
          </div>
        </div>
      </section>

      <section className="panel">
        <div className="section-header">
          <div>
            <p className="section-kicker">Scatter / Bubble</p>
            <div className="ranking-title-row">
              <h2>Composite score vs average wait</h2>
              <MetricInfo description={compositeExplanation} />
            </div>
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
            Rows are intersections and columns are MOEs. Darker cells mean the value is in a higher quartile for that MOE.
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
  const [hoveredDay, setHoveredDay] = useState<string | null>(null);
  const [hoveredHour, setHoveredHour] = useState<number | null>(null);
  const [viewMode, setViewMode] = useState<"table" | "surface3d">("table");
  const [surfaceRevision, setSurfaceRevision] = useState(0);
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
  const selectedMetricLabel = metricOptions.find((item) => item.key === selectedMetric)?.label ?? selectedMetric;
  const hottestCells = useMemo(
    () =>
      [...(dayHour?.cells ?? [])]
        .filter((cell): cell is DayHourCell & { value: number } => cell.value !== null)
        .sort((left, right) => right.value - left.value)
        .slice(0, 3),
    [dayHour?.cells]
  );
  const hourNumbers = useMemo(
    () => (dayHour?.hours ?? []).map((hour) => Number(hour.split(":")[0])),
    [dayHour?.hours]
  );
  const hasSurfaceValues = useMemo(
    () => (dayHour?.cells ?? []).some((cell) => cell.value !== null),
    [dayHour?.cells]
  );
  const surfaceData = useMemo(() => {
    const days = dayHour?.days ?? [];
    const hours = dayHour?.hours ?? [];
    const x = hours.map((hour) => Number(hour.split(":")[0]));
    const y = days.map((_, index) => index);
    const z = days.map((dayName) =>
      hours.map((hourLabel) => {
        const hour = Number(hourLabel.split(":")[0]);
        return cellLookup.get(`${dayName}-${hour}`)?.value ?? null;
      })
    );
    const customdata = days.map((dayName) =>
      hours.map((hourLabel) => {
        const hour = Number(hourLabel.split(":")[0]);
        const cell = cellLookup.get(`${dayName}-${hour}`);
        return [dayName, hourLabel, cell?.sampleCount ?? 0];
      })
    );
    return { x, y, z, customdata };
  }, [cellLookup, dayHour?.days, dayHour?.hours]);
  const focusedCell =
    (hoveredDay && hoveredHour !== null ? cellLookup.get(`${hoveredDay}-${hoveredHour}`) : null) ??
    hottestCells[0] ??
    null;

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
      {loading ? <div className="status-banner">Loading day-hour aggregation from the precomputed MOE outputs...</div> : null}

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
            <CustomSelect
              value={filters.timeOfDayPreset}
              options={timeOfDayPresets.map((preset) => ({ value: preset.id, label: preset.label }))}
              onChange={(nextValue) => {
                const nextPreset = timeOfDayPresets.find((item) => item.id === nextValue);
                setFilters((current) => ({
                  ...current,
                  timeOfDayPreset: nextValue,
                  hourFrom: nextPreset?.start ?? current.hourFrom,
                  hourTo: nextPreset?.end ?? current.hourTo
                }));
              }}
              panelClassName="custom-select-panel--tall"
            />
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
            <CustomSelect
              value={filters.hourFrom}
              options={hourOptions.map((hour) => ({ value: hour, label: hour }))}
              onChange={(nextValue) => setFilters((current) => ({ ...current, hourFrom: nextValue, timeOfDayPreset: "custom" }))}
              panelClassName="custom-select-panel--hours"
            />
          </label>
          <label>
            <span>Hour to</span>
            <CustomSelect
              value={filters.hourTo}
              options={hourOptions.map((hour) => ({ value: hour, label: hour }))}
              onChange={(nextValue) => setFilters((current) => ({ ...current, hourTo: nextValue, timeOfDayPreset: "custom" }))}
              panelClassName="custom-select-panel--hours"
            />
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
              <CustomSelect
                value={selectedSignalId}
                options={(dayHour?.meta.availableSignals ?? []).map((signal) => ({
                  value: signal.id,
                  label: `${signal.id} · ${signal.name}`
                }))}
                onChange={onSignalChange}
                panelClassName="custom-select-panel--signals"
              />
            </label>
            <label>
              <span>MOE</span>
              <CustomSelect
                value={selectedMetric}
                options={metricOptions.map((option) => ({ value: option.key, label: option.label }))}
                onChange={(nextValue) => onMetricChange(nextValue as Exclude<RankingMetricKey, "compositeScore">)}
              />
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
          <p className="section-copy">Each cell aggregates matching day-hour combinations across the selected date range. Darker green means higher {selectedMetricLabel.toLowerCase()}.</p>
        </div>
        <div className="day-hour-explainer">
          <div className="day-hour-legend">
            <span>Lower</span>
            <div className="day-hour-legend-bar" />
            <span>Higher</span>
          </div>
          <div className="day-hour-view-toggle" role="tablist" aria-label="Day-hour view mode">
            <button
              type="button"
              className={viewMode === "table" ? "active" : ""}
              onClick={() => setViewMode("table")}
            >
              Table
            </button>
            <button
              type="button"
              className={viewMode === "surface3d" ? "active" : ""}
              onClick={() => setViewMode("surface3d")}
            >
              3D
            </button>
            {viewMode === "surface3d" ? (
              <button type="button" onClick={() => setSurfaceRevision((current) => current + 1)}>
                Reset camera
              </button>
            ) : null}
          </div>
          <div className="day-hour-hotspots">
            <strong>Top 3 cells</strong>
            <ul>
              {hottestCells.map((cell) => (
                <li key={`${cell.dayName}-${cell.hour}`}>
                  {cell.dayName}, {cell.label}: {cell.value.toFixed(1)} ({cell.sampleCount} samples)
                </li>
              ))}
            </ul>
          </div>
        </div>
        {focusedCell ? (
          <div className="day-hour-focus-card">
            <span>Focused cell</span>
            <strong>
              {focusedCell.dayName}, {focusedCell.label}
            </strong>
            <p>
              {focusedCell.value === null ? "No data" : `${focusedCell.value.toFixed(1)} ${selectedMetricLabel}`}
              {" · "}
              {focusedCell.sampleCount} matching buckets
            </p>
          </div>
        ) : null}
        {viewMode === "surface3d" ? (
          hasSurfaceValues ? (
          <div className="day-hour-surface-shell">
            <Plot
              data={[
                {
                  type: "surface",
                  x: surfaceData.x,
                  y: surfaceData.y,
                  z: surfaceData.z,
                  customdata: surfaceData.customdata,
                  connectgaps: false,
                  colorscale: [
                    [0, "#edf5ef"],
                    [0.2, "#b8d7c1"],
                    [0.45, "#6ca27c"],
                    [0.7, "#3f7653"],
                    [1, "#1c4a33"]
                  ],
                  colorbar: {
                    title: selectedMetricLabel,
                    thickness: 14
                  },
                  hovertemplate:
                    "<b>%{customdata[0]}</b><br>Hour %{customdata[1]}<br>Value %{z:.1f}<br>Samples %{customdata[2]}<extra></extra>"
                }
              ]}
              layout={{
                autosize: true,
                height: 560,
                paper_bgcolor: "rgba(0,0,0,0)",
                plot_bgcolor: "rgba(0,0,0,0)",
                margin: { l: 10, r: 10, t: 10, b: 10 },
                scene: {
                  xaxis: {
                    title: "Hour",
                    tickmode: "array",
                    tickvals: hourNumbers,
                    ticktext: dayHour?.hours ?? [],
                    tickangle: -35,
                    gridcolor: "rgba(79, 122, 93, 0.2)"
                  },
                  yaxis: {
                    title: "Day",
                    tickmode: "array",
                    tickvals: (dayHour?.days ?? []).map((_, index) => index),
                    ticktext: dayHour?.days ?? [],
                    autorange: "reversed",
                    gridcolor: "rgba(79, 122, 93, 0.2)"
                  },
                  zaxis: {
                    title: selectedMetricLabel,
                    gridcolor: "rgba(79, 122, 93, 0.2)"
                  },
                  camera: {
                    eye: { x: 1.55, y: -1.9, z: 0.95 }
                  }
                }
              }}
              config={{
                displaylogo: false,
                responsive: true,
                modeBarButtonsToRemove: ["lasso2d", "select2d", "toImage"]
              }}
              revision={surfaceRevision}
              className="day-hour-surface-plot"
              onHover={(event: { points?: Array<{ customdata?: [string, string, number] }> }) => {
                const point = event.points?.[0];
                const custom = point?.customdata as [string, string, number] | undefined;
                if (!custom) {
                  return;
                }
                setHoveredDay(custom[0]);
                setHoveredHour(Number(custom[1].split(":")[0]));
              }}
              onUnhover={() => {
                setHoveredDay(null);
                setHoveredHour(null);
              }}
              onClick={(event: { points?: Array<{ customdata?: [string, string, number] }> }) => {
                const point = event.points?.[0];
                const custom = point?.customdata as [string, string, number] | undefined;
                if (!custom) {
                  return;
                }
                setHoveredDay(custom[0]);
                setHoveredHour(Number(custom[1].split(":")[0]));
              }}
            />
          </div>
          ) : (
            <div className="empty-chart-state">
              <strong>No 3D surface is available for this signal/metric yet.</strong>
              <p>
                The selected day-hour window does not have enough non-null values to render a rotatable surface.
                Try a different intersection, metric, or wider date range.
              </p>
            </div>
          )
        ) : (
          <div className="table-wrap">
            <table className="day-hour-table">
              <thead>
                <tr>
                  <th>Day</th>
                  {(dayHour?.hours ?? []).map((hour) => (
                    <th
                      key={hour}
                      className={hoveredHour === Number(hour.split(":")[0]) ? "is-hovered-col" : ""}
                    >
                      {hour}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(dayHour?.days ?? []).map((dayName) => (
                  <tr key={dayName}>
                    <td className={`day-hour-label ${hoveredDay === dayName ? "is-hovered-row" : ""}`}>{dayName}</td>
                    {(dayHour?.hours ?? []).map((label) => {
                      const hour = Number(label.split(":")[0]);
                      const cell = cellLookup.get(`${dayName}-${hour}`);
                      return (
                        <td
                          key={`${dayName}-${label}`}
                          className={`day-hour-cell ${hoveredDay === dayName ? "is-hovered-row" : ""} ${hoveredHour === hour ? "is-hovered-col" : ""}`}
                          style={{ backgroundColor: heatColor(cell?.value ?? null) }}
                          title={
                            cell?.value === null
                              ? `${dayName} ${label}: no data`
                              : `${dayName} ${label}: ${(cell?.value ?? 0).toFixed(1)} across ${cell?.sampleCount ?? 0} matching hourly buckets`
                          }
                          onMouseEnter={() => {
                            setHoveredDay(dayName);
                            setHoveredHour(hour);
                          }}
                          onMouseLeave={() => {
                            setHoveredDay(null);
                            setHoveredHour(null);
                          }}
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
        )}
      </section>
    </main>
  );
}

function clusterTone(clusterId: number | null, sampleCount: number, isOutlier: boolean) {
  if (sampleCount === 0 || clusterId === null) {
    return "#f5f2ee";
  }
  if (isOutlier || clusterId === -1) {
    return "#ffe6dc";
  }
  const palette = ["#e8f0ff", "#f4e7ff", "#e7f8f0", "#fff0dc", "#fce5ef", "#e7f5fb", "#efeccf"];
  return palette[Math.abs(clusterId) % palette.length];
}

function BottleneckClusterView({
  data,
  loading,
  error,
  selectedSignalId,
  onSignalChange,
  selectedPhase,
  onPhaseChange,
  selectedMoes,
  onMoeToggle,
  eps,
  onEpsChange,
  minSamples,
  onMinSamplesChange,
}: {
  data: BottleneckResponse | null;
  loading: boolean;
  error: string | null;
  selectedSignalId: string;
  onSignalChange: (value: string) => void;
  selectedPhase: string;
  onPhaseChange: (value: string) => void;
  selectedMoes: string[];
  onMoeToggle: (value: string) => void;
  eps: string;
  onEpsChange: (value: string) => void;
  minSamples: string;
  onMinSamplesChange: (value: string) => void;
}) {
  const slotLookup = useMemo(
    () => new Map((data?.slots ?? []).map((slot) => [`${slot.dayName}-${slot.hour}`, slot])),
    [data?.slots]
  );
  const moeOptions = data?.meta.moeOptions ?? [];
  const ignoredMoeLabels = moeOptions
    .filter((option) => selectedMoes.includes(option.key) && !(data?.filters.usedMoes ?? []).includes(option.key))
    .map((option) => option.label);
  const dbscanPresets = [
    { id: "tight", label: "Tight", eps: "0.20", minSamples: "2" },
    { id: "balanced", label: "Balanced", eps: "0.25", minSamples: "2" },
    { id: "broad", label: "Broad", eps: "0.35", minSamples: "3" },
  ];

  return (
    <main className="workspace ranking-workspace">
      {error ? <div className="status-banner error">{error}</div> : null}
      {loading ? <div className="status-banner">Clustering phase-level hourly MOE vectors...</div> : null}

      <section className="filters-panel">
        <div className="section-header">
          <div>
            <p className="section-kicker">Task 3</p>
            <h2>Intersection bottleneck clustering</h2>
          </div>
          <p className="section-copy">Each cell represents one day-hour slot aggregated across all matching weeks, then clustered with DBSCAN.</p>
        </div>

        <div className="filters-grid filters-grid-v1">
          <label>
            <span>Intersection</span>
            <CustomSelect
              value={selectedSignalId}
              options={(data?.meta.availableSignals ?? []).map((signal) => ({
                value: signal.id,
                label: `${signal.id} · ${signal.name}`,
              }))}
              onChange={onSignalChange}
              panelClassName="custom-select-panel--signals"
            />
          </label>
          <label>
            <span>Date from</span>
            <input type="date" value={data?.filters.dateFrom ?? ""} readOnly />
          </label>
          <label>
            <span>Date to</span>
            <input type="date" value={data?.filters.dateTo ?? ""} readOnly />
          </label>
          <div className="filter-summary">
            <p className="filter-summary-label">Algorithm</p>
            <p>{data?.meta.algorithm.name ?? "DBSCAN"}</p>
            <strong>
              eps {data?.meta.algorithm.eps ?? "—"} · min samples {data?.meta.algorithm.minSamples ?? "—"}
            </strong>
          </div>
          <label>
            <span>DBSCAN eps</span>
            <input
              type="number"
              min="0.1"
              max="5"
              step="any"
              value={eps}
              onChange={(event) => onEpsChange(event.target.value)}
            />
          </label>
          <label>
            <span>Min samples</span>
            <input
              type="number"
              min="1"
              max="12"
              step="1"
              value={minSamples}
              onChange={(event) => onMinSamplesChange(event.target.value)}
            />
          </label>
        </div>

        <div className="days-row">
          <p className="filter-summary-label">Suggested presets</p>
          <div className="day-chip-row">
            {dbscanPresets.map((preset) => {
              const active = eps === preset.eps && minSamples === preset.minSamples;
              return (
                <button
                  key={preset.id}
                  type="button"
                  className={`day-chip bottleneck-moe-chip ${active ? "active" : ""}`}
                  onClick={() => {
                    onEpsChange(preset.eps);
                    onMinSamplesChange(preset.minSamples);
                  }}
                  title={`eps ${preset.eps}, min samples ${preset.minSamples}`}
                >
                  {preset.label}
                </button>
              );
            })}
          </div>
        </div>

        <div className="days-row">
          <p className="filter-summary-label">Phase</p>
          <div className="day-chip-row phase-chip-row bottleneck-phase-chip-row">
            {(data?.availablePhases ?? []).map((phase) => {
              const value = String(phase);
              return (
                <button
                  key={phase}
                  type="button"
                  className={`day-chip phase-chip ${selectedPhase === value ? "active" : ""}`}
                  onClick={() => onPhaseChange(value)}
                >
                  P{phase}
                </button>
              );
            })}
          </div>
        </div>

        <div className="days-row">
          <p className="filter-summary-label">MOEs to cluster</p>
          <div className="day-chip-row">
            {moeOptions.map((option) => {
              const active = selectedMoes.includes(option.key);
              return (
                <button
                  key={option.key}
                  type="button"
                  className={`day-chip bottleneck-moe-chip ${active ? "active" : ""}`}
                  onClick={() => onMoeToggle(option.key)}
                  title={option.description}
                >
                  {option.label}
                </button>
              );
            })}
          </div>
        </div>
        {ignoredMoeLabels.length ? (
          <p className="history-note">
            Ignored for this phase because no usable hourly values were found: {ignoredMoeLabels.join(", ")}.
          </p>
        ) : null}
      </section>

      <section className="panel">
        <div className="historical-summary-grid">
          <article className="history-stat">
            <span>Signal</span>
            <strong>{data?.signal.id ?? "—"}</strong>
          </article>
          <article className="history-stat">
            <span>Slots with data</span>
            <strong>{data?.summary.slotsWithData ?? 0}</strong>
          </article>
          <article className="history-stat">
            <span>Clusters found</span>
            <strong>{data?.summary.clusterCount ?? 0}</strong>
          </article>
          <article className="history-stat">
            <span>Outliers</span>
            <strong>{data?.summary.outlierCount ?? 0}</strong>
          </article>
          <article className="history-stat">
            <span>Largest shifts</span>
            <strong>{data?.summary.changeCount ?? 0}</strong>
          </article>
        </div>
      </section>

      <section className="panel">
        <div className="section-header">
          <div>
            <p className="section-kicker">Change Detection</p>
            <h2>Largest hour-to-hour shifts</h2>
          </div>
          <p className="section-copy">These are the strongest adjacent time-slot changes for the selected phase, ranked by MOE movement and cluster transitions.</p>
        </div>
        <div className="insight-grid">
          {(data?.changeDetections ?? []).map((change) => (
            <article key={`${change.dayName}-${change.fromHour}-${change.toHour}`} className="insight-card">
              <div className="insight-top">
                <div className="insight-heading">
                  <span className="insight-label">{change.dayName}</span>
                </div>
                <strong className="insight-value">
                  {change.fromLabel} to {change.toLabel}
                </strong>
              </div>
              <p className="bottleneck-cluster-tag">
                {change.severityLabel} · {change.transitionType}
              </p>
              <p className="bottleneck-cluster-summary">{change.summarySentence}</p>
              <div className="day-chip-row">
                <span className="day-chip bottleneck-summary-chip active">{change.fromClusterLabel}</span>
                <span className="day-chip bottleneck-summary-chip active">{change.toClusterLabel}</span>
                <span className="day-chip bottleneck-summary-chip active">Score {change.severityScore.toFixed(2)}</span>
              </div>
              {change.topChanges.length ? (
                <p className="bottleneck-cluster-slots">
                  <strong>Main changes:</strong> {change.topChanges.join(", ")}
                </p>
              ) : null}
            </article>
          ))}
          {data?.changeDetections.length ? null : (
            <article className="insight-card">
              <div className="insight-top">
                <div className="insight-heading">
                  <span className="insight-label">No large shifts yet</span>
                </div>
              </div>
              <p>The selected intersection and phase did not produce enough adjacent hourly changes to flag with this first-pass change detector.</p>
            </article>
          )}
        </div>
      </section>

      <section className="panel">
        <div className="section-header">
          <div>
            <p className="section-kicker">Cluster Grid</p>
            <h2>Day-hour slot clusters</h2>
          </div>
          <p className="section-copy">`C1`, `C2`, etc. are DBSCAN clusters. `O` marks outliers and `—` means no usable data for that slot.</p>
        </div>
        <div className="table-wrap">
          <table className="day-hour-table bottleneck-table">
            <thead>
              <tr>
                <th>Day</th>
                {(data?.hours ?? []).map((hour) => (
                  <th key={hour}>{hour}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {(data?.days ?? []).map((dayName) => (
                <tr key={dayName}>
                  <td className="day-hour-label">{dayName}</td>
                  {(data?.hours ?? []).map((label) => {
                    const hour = Number(label.split(":")[0]);
                    const slot = slotLookup.get(`${dayName}-${hour}`);
                    const text =
                      !slot || slot.sampleCount === 0
                        ? "—"
                        : slot.isOutlier || slot.clusterId === -1
                          ? "O"
                          : `C${(slot.clusterId ?? 0) + 1}`;
                    return (
                      <td
                        key={`${dayName}-${label}`}
                        className="bottleneck-cell"
                        style={{ backgroundColor: clusterTone(slot?.clusterId ?? null, slot?.sampleCount ?? 0, slot?.isOutlier ?? false) }}
                        title={
                          !slot
                            ? `${dayName} ${label}: no data`
                            : `${dayName} ${label}: ${slot.clusterLabel} · ${slot.sampleCount} samples`
                        }
                      >
                        <strong>{text}</strong>
                        <span>{slot?.sampleCount ?? 0}</span>
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <div className="section-header">
          <div>
            <p className="section-kicker">Cluster Notes</p>
            <h2>Cluster summaries</h2>
          </div>
          <p className="section-copy">These summaries show which slots were grouped together and the mean MOE profile for each cluster.</p>
        </div>
        <div className="insight-grid">
          {(data?.clusterSummaries ?? []).map((cluster) => (
            <article key={cluster.clusterId} className="insight-card">
              <div className="insight-top">
                <div className="insight-heading">
                  <span className="insight-label">{cluster.clusterLabel}</span>
                </div>
                <strong className="insight-value">{cluster.slotCount} slots</strong>
              </div>
              <p className="bottleneck-cluster-tag">{cluster.performanceTag ?? "Mixed pattern"}</p>
              <p className="bottleneck-cluster-summary">
                {cluster.summarySentence ??
                  `${cluster.dayPattern ?? "Mixed days"} ${String(cluster.timeBand ?? "mixed timing").toLowerCase()}.`}
              </p>
              <div className="day-chip-row">
                {(data?.filters.usedMoes ?? []).map((key) => {
                  const option = data?.meta.moeOptions.find((item) => item.key === key);
                  const value = cluster[key];
                  return (
                    <span key={`${cluster.clusterId}-${key}`} className="day-chip bottleneck-summary-chip active">
                      {option?.label ?? key}: {typeof value === "number" ? value.toFixed(1) : "—"}
                    </span>
                  );
                })}
              </div>
              <p className="bottleneck-cluster-slots">
                <strong>Example slots:</strong> {((cluster.slotsPreview as string[]) ?? []).join(", ")}
              </p>
              {Array.isArray(cluster.slots) && cluster.slots.length > 8 ? (
                <p>+ {cluster.slots.length - 8} more slots in this cluster.</p>
              ) : null}
            </article>
          ))}
          {data?.clusterSummaries.length ? null : (
            <article className="insight-card">
              <div className="insight-top">
                <div className="insight-heading">
                  <span className="insight-label">No clusters yet</span>
                </div>
              </div>
              <p>The current intersection, phase, and MOE combination did not produce enough populated slots for DBSCAN to form clusters.</p>
            </article>
          )}
        </div>
      </section>
    </main>
  );
}

function HdbscanClusterView({
  data,
  loading,
  error,
  selectedSignalIds,
  onToggleSignal,
  onApplySignalPreset,
  selectedPhases,
  onTogglePhase,
  selectedMoes,
  onMoeToggle,
  minClusterSize,
  onMinClusterSizeChange,
  minSamples,
  onMinSamplesChange,
  clusterSelectionEpsilon,
  onClusterSelectionEpsilonChange,
}: {
  data: HdbscanResponse | null;
  loading: boolean;
  error: string | null;
  selectedSignalIds: string[];
  onToggleSignal: (value: string) => void;
  onApplySignalPreset: (ids: string[]) => void;
  selectedPhases: string[];
  onTogglePhase: (value: string) => void;
  selectedMoes: string[];
  onMoeToggle: (value: string) => void;
  minClusterSize: string;
  onMinClusterSizeChange: (value: string) => void;
  minSamples: string;
  onMinSamplesChange: (value: string) => void;
  clusterSelectionEpsilon: string;
  onClusterSelectionEpsilonChange: (value: string) => void;
}) {
  const slotLookup = useMemo(
    () => new Map((data?.slots ?? []).map((slot) => [`${slot.dayName}-${slot.hour}`, slot])),
    [data?.slots]
  );
  const availableSignals = data?.meta.availableSignals ?? [];
  const moeOptions = data?.meta.moeOptions ?? [];
  const ignoredMoeLabels = moeOptions
    .filter((option) => selectedMoes.includes(option.key) && !(data?.filters.usedMoes ?? []).includes(option.key))
    .map((option) => option.label);
  const selectedSet = new Set(selectedSignalIds);
  const orderedSignals = availableSignals;
  const mapCenter = useMemo<LatLngExpression>(() => {
    const active = orderedSignals.filter((signal) => selectedSet.has(signal.id));
    const base = active.length ? active : orderedSignals;
    if (!base.length) {
      return [27.2989, -82.48];
    }
    const lat = base.reduce((sum, signal) => sum + signal.lat, 0) / base.length;
    const lon = base.reduce((sum, signal) => sum + signal.lon, 0) / base.length;
    return [lat, lon];
  }, [orderedSignals, selectedSignalIds.join(",")]);
  const mapPath = useMemo<LatLngExpression[]>(
    () => orderedSignals.map((signal) => [signal.lat, signal.lon] as LatLngExpression),
    [orderedSignals]
  );
  const hdbscanPresets = [
    { id: "compact", label: "Compact", minClusterSize: "4", minSamples: "2", epsilon: "0" },
    { id: "balanced", label: "Balanced", minClusterSize: "6", minSamples: "3", epsilon: "0.05" },
    { id: "loose", label: "Loose", minClusterSize: "8", minSamples: "3", epsilon: "0.08" },
  ];

  return (
    <main className="workspace ranking-workspace">
      {error ? <div className="status-banner error">{error}</div> : null}
      {loading ? <div className="status-banner">Clustering multi-intersection phase-level hourly MOE vectors...</div> : null}

      <section className="map-panel">
        <div className="section-header">
          <div>
            <p className="section-kicker">Task 3</p>
            <h2>Multi-intersection HDBSCAN clustering</h2>
          </div>
          <p className="section-copy">Select multiple intersections and phases, then cluster the aggregated day-hour slot patterns across the chosen group.</p>
        </div>

        <div className="map-frame">
          <MapContainer center={mapCenter} zoom={12} scrollWheelZoom={false} className="leaflet-map">
            <RankingMapFocus center={mapCenter} zoom={selectedSignalIds.length === 1 ? 15 : 12} />
            <TileLayer
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />
            {mapPath.length > 1 ? (
              <Polyline positions={mapPath} pathOptions={{ color: "#c98358", weight: 5, opacity: 0.7 }} />
            ) : null}
            {orderedSignals.map((signal) => {
              const active = selectedSet.has(signal.id);
              return (
                <Fragment key={signal.id}>
                  <Marker
                    position={[signal.lat, signal.lon]}
                    icon={buildMarkerIcon("watch", active)}
                    eventHandlers={{ click: () => onToggleSignal(signal.id) }}
                  >
                    <Popup>
                      <strong>{signal.name}</strong>
                      <br />
                      Signal ID {signal.id}
                    </Popup>
                  </Marker>
                  {active ? (
                    <CircleMarker
                      center={[signal.lat, signal.lon]}
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
            <p className="list-note">{selectedSignalIds.length} selected for HDBSCAN.</p>
            <div className="preset-row">
              {(data?.meta.selectionPresets ?? []).map((preset) => (
                <button
                  key={preset.id}
                  type="button"
                  className="preset-chip"
                  onClick={() => onApplySignalPreset(preset.intersectionIds)}
                >
                  {preset.label}
                </button>
              ))}
            </div>
            {orderedSignals.map((signal) => {
              const active = selectedSet.has(signal.id);
              return (
                <label key={signal.id} className={`intersection-row tone-watch ${active ? "active" : ""}`}>
                  <input type="checkbox" checked={active} onChange={() => onToggleSignal(signal.id)} />
                  <span className="row-index">{signal.id}</span>
                  <div className="intersection-copy">
                    <span className="intersection-name">{signal.name}</span>
                    <span className="intersection-meta">{signal.region}</span>
                  </div>
                </label>
              );
            })}
          </aside>
        </div>
      </section>

      <section className="filters-panel">
        <div className="filters-grid filters-grid-v1">
          <label>
            <span>Date from</span>
            <input type="date" value={data?.filters.dateFrom ?? ""} readOnly />
          </label>
          <label>
            <span>Date to</span>
            <input type="date" value={data?.filters.dateTo ?? ""} readOnly />
          </label>
          <div className="filter-summary">
            <p className="filter-summary-label">Algorithm</p>
            <p>{data?.meta.algorithm.name ?? "HDBSCAN"}</p>
            <strong>
              min cluster {data?.meta.algorithm.minClusterSize ?? "—"} · min samples {data?.meta.algorithm.minSamples ?? "—"}
            </strong>
          </div>
          <label>
            <span>Min cluster size</span>
            <input type="number" min="2" max="40" step="1" value={minClusterSize} onChange={(event) => onMinClusterSizeChange(event.target.value)} />
          </label>
          <label>
            <span>Min samples</span>
            <input type="number" min="1" max="20" step="1" value={minSamples} onChange={(event) => onMinSamplesChange(event.target.value)} />
          </label>
          <label>
            <span>Selection epsilon</span>
            <input type="number" min="0" max="5" step="any" value={clusterSelectionEpsilon} onChange={(event) => onClusterSelectionEpsilonChange(event.target.value)} />
          </label>
        </div>

        <div className="days-row">
          <p className="filter-summary-label">Suggested presets</p>
          <div className="day-chip-row">
            {hdbscanPresets.map((preset) => {
              const active =
                minClusterSize === preset.minClusterSize &&
                minSamples === preset.minSamples &&
                clusterSelectionEpsilon === preset.epsilon;
              return (
                <button
                  key={preset.id}
                  type="button"
                  className={`day-chip bottleneck-moe-chip ${active ? "active" : ""}`}
                  onClick={() => {
                    onMinClusterSizeChange(preset.minClusterSize);
                    onMinSamplesChange(preset.minSamples);
                    onClusterSelectionEpsilonChange(preset.epsilon);
                  }}
                >
                  {preset.label}
                </button>
              );
            })}
          </div>
        </div>

        <div className="days-row">
          <p className="filter-summary-label">Phases</p>
          <div className="day-chip-row bottleneck-phase-chip-row">
            {(data?.availablePhases ?? []).map((phase) => {
              const value = String(phase);
              const active = selectedPhases.includes(value);
              return (
                <button
                  key={phase}
                  type="button"
                  className={`day-chip phase-chip ${active ? "active" : ""}`}
                  onClick={() => onTogglePhase(value)}
                >
                  P{phase}
                </button>
              );
            })}
          </div>
        </div>

        <div className="days-row">
          <p className="filter-summary-label">MOEs to cluster</p>
          <div className="day-chip-row">
            {moeOptions.map((option) => {
              const active = selectedMoes.includes(option.key);
              return (
                <button
                  key={option.key}
                  type="button"
                  className={`day-chip bottleneck-moe-chip ${active ? "active" : ""}`}
                  onClick={() => onMoeToggle(option.key)}
                  title={option.description}
                >
                  {option.label}
                </button>
              );
            })}
          </div>
        </div>
        {ignoredMoeLabels.length ? (
          <p className="history-note">
            Ignored for the current selection because no usable hourly values were found: {ignoredMoeLabels.join(", ")}.
          </p>
        ) : null}
      </section>

      <section className="panel">
        <div className="historical-summary-grid">
          <article className="history-stat">
            <span>Signals</span>
            <strong>{data?.summary.selectedSignalCount ?? 0}</strong>
          </article>
          <article className="history-stat">
            <span>Phases</span>
            <strong>{data?.summary.selectedPhaseCount ?? 0}</strong>
          </article>
          <article className="history-stat">
            <span>Slots with data</span>
            <strong>{data?.summary.slotsWithData ?? 0}</strong>
          </article>
          <article className="history-stat">
            <span>Clusters found</span>
            <strong>{data?.summary.clusterCount ?? 0}</strong>
          </article>
          <article className="history-stat">
            <span>Outliers</span>
            <strong>{data?.summary.outlierCount ?? 0}</strong>
          </article>
          <article className="history-stat">
            <span>Largest shifts</span>
            <strong>{data?.summary.changeCount ?? 0}</strong>
          </article>
        </div>
      </section>

      <section className="panel">
        <div className="section-header">
          <div>
            <p className="section-kicker">Change Detection</p>
            <h2>Largest hour-to-hour shifts</h2>
          </div>
          <p className="section-copy">These are the strongest adjacent time-slot changes across the selected intersections and phases.</p>
        </div>
        <div className="insight-grid">
          {(data?.changeDetections ?? []).map((change) => (
            <article key={`${change.dayName}-${change.fromHour}-${change.toHour}`} className="insight-card">
              <div className="insight-top">
                <div className="insight-heading">
                  <span className="insight-label">{change.dayName}</span>
                </div>
                <strong className="insight-value">
                  {change.fromLabel} to {change.toLabel}
                </strong>
              </div>
              <p className="bottleneck-cluster-tag">
                {change.severityLabel} · {change.transitionType}
              </p>
              <p className="bottleneck-cluster-summary">{change.summarySentence}</p>
              <div className="day-chip-row">
                <span className="day-chip bottleneck-summary-chip active">{change.fromClusterLabel}</span>
                <span className="day-chip bottleneck-summary-chip active">{change.toClusterLabel}</span>
                <span className="day-chip bottleneck-summary-chip active">Score {change.severityScore.toFixed(2)}</span>
              </div>
            </article>
          ))}
          {data?.changeDetections.length ? null : (
            <article className="insight-card">
              <div className="insight-top">
                <div className="insight-heading">
                  <span className="insight-label">No large shifts yet</span>
                </div>
              </div>
              <p>The current multi-intersection selection did not produce enough adjacent hourly changes to flag with this first-pass change detector.</p>
            </article>
          )}
        </div>
      </section>

      <section className="panel">
        <div className="section-header">
          <div>
            <p className="section-kicker">Cluster Grid</p>
            <h2>Day-hour slot clusters</h2>
          </div>
          <p className="section-copy">`C1`, `C2`, etc. are HDBSCAN clusters. `O` marks outliers and `—` means no usable data for that slot.</p>
        </div>
        <div className="table-wrap">
          <table className="day-hour-table bottleneck-table">
            <thead>
              <tr>
                <th>Day</th>
                {(data?.hours ?? []).map((hour) => (
                  <th key={hour}>{hour}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {(data?.days ?? []).map((dayName) => (
                <tr key={dayName}>
                  <td className="day-hour-label">{dayName}</td>
                  {(data?.hours ?? []).map((label) => {
                    const hour = Number(label.split(":")[0]);
                    const slot = slotLookup.get(`${dayName}-${hour}`);
                    const text =
                      !slot || slot.sampleCount === 0
                        ? "—"
                        : slot.isOutlier || slot.clusterId === -1
                          ? "O"
                          : `C${(slot.clusterId ?? 0) + 1}`;
                    return (
                      <td
                        key={`${dayName}-${label}`}
                        className="bottleneck-cell"
                        style={{ backgroundColor: clusterTone(slot?.clusterId ?? null, slot?.sampleCount ?? 0, slot?.isOutlier ?? false) }}
                        title={
                          !slot
                            ? `${dayName} ${label}: no data`
                            : `${dayName} ${label}: ${slot.clusterLabel} · ${slot.sampleCount} samples`
                        }
                      >
                        <strong>{text}</strong>
                        <span>{slot?.sampleCount ?? 0}</span>
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="panel">
        <div className="section-header">
          <div>
            <p className="section-kicker">Cluster Notes</p>
            <h2>Cluster summaries</h2>
          </div>
          <p className="section-copy">These summaries show which slots were grouped together and the mean MOE profile for each cluster.</p>
        </div>
        <div className="insight-grid">
          {(data?.clusterSummaries ?? []).map((cluster) => (
            <article key={cluster.clusterId} className="insight-card">
              <div className="insight-top">
                <div className="insight-heading">
                  <span className="insight-label">{cluster.clusterLabel}</span>
                </div>
                <strong className="insight-value">{cluster.slotCount} slots</strong>
              </div>
              <p className="bottleneck-cluster-tag">{cluster.performanceTag ?? "Mixed pattern"}</p>
              <p className="bottleneck-cluster-summary">
                {cluster.summarySentence ??
                  `${cluster.dayPattern ?? "Mixed days"} ${String(cluster.timeBand ?? "mixed timing").toLowerCase()}.`}
              </p>
              <div className="day-chip-row">
                {(data?.filters.usedMoes ?? []).map((key) => {
                  const option = data?.meta.moeOptions.find((item) => item.key === key);
                  const value = cluster[key];
                  return (
                    <span key={`${cluster.clusterId}-${key}`} className="day-chip bottleneck-summary-chip active">
                      {option?.label ?? key}: {typeof value === "number" ? value.toFixed(1) : "—"}
                    </span>
                  );
                })}
              </div>
              <p className="bottleneck-cluster-slots">
                <strong>Example slots:</strong> {((cluster.slotsPreview as string[]) ?? []).join(", ")}
              </p>
              {Array.isArray(cluster.slots) && cluster.slots.length > 8 ? (
                <p>+ {cluster.slots.length - 8} more slots in this cluster.</p>
              ) : null}
            </article>
          ))}
          {data?.clusterSummaries.length ? null : (
            <article className="insight-card">
              <div className="insight-top">
                <div className="insight-heading">
                  <span className="insight-label">No clusters yet</span>
                </div>
              </div>
              <p>The current multi-intersection selection did not produce enough populated slots for HDBSCAN to form clusters.</p>
            </article>
          )}
        </div>
      </section>
    </main>
  );
}

function ChangeDetectionView({
  data,
  loading,
  error,
  selectedSignalIds,
  onToggleSignal,
  onApplySignalPreset,
  selectedMoe,
  onMoeChange,
  granularity,
  onGranularityChange,
  selectedDate,
  onSelectedDateChange,
  selectedHour,
  onSelectedHourChange,
}: {
  data: ChangeDetectionResponse | null;
  loading: boolean;
  error: string | null;
  selectedSignalIds: string[];
  onToggleSignal: (value: string) => void;
  onApplySignalPreset: (ids: string[]) => void;
  selectedMoe: Exclude<RankingMetricKey, "compositeScore">;
  onMoeChange: (value: Exclude<RankingMetricKey, "compositeScore">) => void;
  granularity: "day" | "hour";
  onGranularityChange: (value: "day" | "hour") => void;
  selectedDate: string;
  onSelectedDateChange: (value: string) => void;
  selectedHour: string;
  onSelectedHourChange: (value: string) => void;
}) {
  const availableSignals = data?.meta.availableSignals ?? [];
  const selectedSet = new Set(selectedSignalIds);
  const orderedSignals = availableSignals;
  const mapStatuses = data?.mapStatuses ?? [];
  const mapStatusLookup = useMemo(() => new Map(mapStatuses.map((status) => [status.signalId, status])), [mapStatuses]);
  const mapCenter = useMemo<LatLngExpression>(() => {
    const active = orderedSignals.filter((signal) => selectedSet.has(signal.id));
    const base = active.length ? active : orderedSignals;
    if (!base.length) {
      return [27.2989, -82.48];
    }
    const lat = base.reduce((sum, signal) => sum + signal.lat, 0) / base.length;
    const lon = base.reduce((sum, signal) => sum + signal.lon, 0) / base.length;
    return [lat, lon];
  }, [orderedSignals, selectedSignalIds.join(",")]);
  const mapPath = useMemo<LatLngExpression[]>(
    () => orderedSignals.map((signal) => [signal.lat, signal.lon] as LatLngExpression),
    [orderedSignals]
  );

  const summaryLookup = useMemo(
    () => new Map((data?.intersectionSummaries ?? []).map((row) => [row.signalId, row])),
    [data?.intersectionSummaries]
  );
  const topChanges = data?.topChanges ?? [];
  const slotRows = data?.slotRows ?? [];
  const selectedMoeLabel =
    data?.meta.moeOptions.find((option) => option.key === selectedMoe)?.label ?? selectedMoe;

  return (
    <main className="workspace ranking-workspace">
      {error ? <div className="status-banner error">{error}</div> : null}
      {loading ? <div className="status-banner">Comparing the selected interval against the historical mean for each selected intersection...</div> : null}

      <section className="map-panel">
        <div className="section-header">
          <div>
            <p className="section-kicker">Task 3</p>
            <h2>Change detection</h2>
          </div>
          <p className="section-copy">
            For the selected MOE, each intersection is compared only against its own historical mean for the selected interval.
          </p>
        </div>

        <div className="map-frame">
          <MapContainer center={mapCenter} zoom={12} scrollWheelZoom={false} className="leaflet-map">
            <RankingMapFocus center={mapCenter} zoom={selectedSignalIds.length === 1 ? 15 : 12} />
            <TileLayer
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />
            {mapPath.length > 1 ? (
              <Polyline positions={mapPath} pathOptions={{ color: "#c98358", weight: 5, opacity: 0.7 }} />
            ) : null}
            {orderedSignals.map((signal) => {
              const active = selectedSet.has(signal.id);
              const mapStatus = mapStatusLookup.get(signal.id);
              return (
                <Fragment key={signal.id}>
                  <Marker
                    position={[signal.lat, signal.lon]}
                    icon={buildChangeMarkerIcon(mapStatus?.tone ?? "calm", active)}
                    eventHandlers={{ click: () => onToggleSignal(signal.id) }}
                  >
                    <Popup>
                      <strong>{signal.name}</strong>
                      <br />
                      Signal ID {signal.id}
                      <br />
                      Delta: {mapStatus?.deltaPct == null ? "—" : `${mapStatus.deltaPct.toFixed(1)}%`}
                    </Popup>
                  </Marker>
                  {active ? (
                    <CircleMarker
                      center={[signal.lat, signal.lon]}
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
            <p className="list-note">{selectedSignalIds.length} selected for monitoring.</p>
            <div className="preset-row">
              {(data?.meta.selectionPresets ?? []).map((preset) => (
                <button
                  key={preset.id}
                  type="button"
                  className="preset-chip"
                  onClick={() => onApplySignalPreset(preset.intersectionIds)}
                >
                  {preset.label}
                </button>
              ))}
            </div>
            {orderedSignals.map((signal) => {
              const active = selectedSet.has(signal.id);
              const summary = summaryLookup.get(signal.id);
              const mapStatus = mapStatusLookup.get(signal.id);
              return (
                <label key={signal.id} className={`intersection-row tone-watch ${active ? "active" : ""}`}>
                  <input type="checkbox" checked={active} onChange={() => onToggleSignal(signal.id)} />
                  <span className="row-index">{signal.id}</span>
                  <div className="intersection-copy">
                    <span className="intersection-name">{signal.name}</span>
                    <span className="intersection-meta">
                      {mapStatus?.deltaPct == null ? "No baseline yet" : `${mapStatus.deltaPct.toFixed(1)}% vs history`}
                    </span>
                  </div>
                </label>
              );
            })}
          </aside>
        </div>
      </section>

      <section className="filters-panel">
        <div className="filters-grid filters-grid-v1">
          <label>
            <span>Selected date</span>
            <CustomSelect
              value={selectedDate}
              options={(data?.availableDates ?? []).map((value) => ({ value, label: value }))}
              onChange={onSelectedDateChange}
              panelClassName="custom-select-panel--hours"
            />
          </label>
          {granularity === "hour" ? (
            <label>
              <span>Selected hour</span>
              <CustomSelect
                value={selectedHour}
                options={(data?.availableHours ?? []).map((value) => ({ value, label: value }))}
                onChange={onSelectedHourChange}
                panelClassName="custom-select-panel--hours"
              />
            </label>
          ) : (
            <div className="filter-summary">
              <p className="filter-summary-label">Selected weekday</p>
              <p>{data?.slotSelection.dayName ?? "—"}</p>
              <strong>Daily comparison</strong>
            </div>
          )}
          <div className="filter-summary">
            <p className="filter-summary-label">Selected slot</p>
            <p>
              {granularity === "day"
                ? `${data?.slotSelection.dayName ?? "—"}`
                : `${selectedHour}`}
            </p>
            <strong>
              {granularity === "day"
                ? selectedDate
                : `${data?.summary.slotPointCount ?? 0} matching rows across dates`}
            </strong>
          </div>
        </div>

        <div className="days-row">
          <p className="filter-summary-label">Granularity</p>
          <div className="day-chip-row">
            {(data?.meta.granularityOptions ?? []).map((option) => {
              const active = granularity === option.key;
              return (
                <button
                  key={option.key}
                  type="button"
                  className={`day-chip bottleneck-moe-chip ${active ? "active" : ""}`}
                  onClick={() => onGranularityChange(option.key)}
                >
                  {option.label}
                </button>
              );
            })}
          </div>
        </div>

        <div className="days-row">
          <p className="filter-summary-label">MOE</p>
          <div className="day-chip-row">
            {(data?.meta.moeOptions ?? []).map((option) => {
              const active = selectedMoe === option.key;
              return (
                <button
                  key={option.key}
                  type="button"
                  className={`day-chip bottleneck-moe-chip ${active ? "active" : ""}`}
                  onClick={() => onMoeChange(option.key)}
                >
                  {option.label}
                </button>
              );
            })}
          </div>
        </div>
      </section>

      <section className="panel">
        <div className="historical-summary-grid bottleneck-summary-grid">
          <article className="history-stat">
            <span>Signals</span>
            <strong>{data?.summary.selectedSignalCount ?? 0}</strong>
          </article>
          <article className="history-stat">
            <span>Points checked</span>
            <strong>{data?.summary.pointCount ?? 0}</strong>
          </article>
          <article className="history-stat">
            <span>Changed ±5%</span>
            <strong>{data?.summary.changeCount ?? 0}</strong>
          </article>
          <article className="history-stat">
            <span>Within threshold</span>
            <strong>{data?.summary.stableCount ?? 0}</strong>
          </article>
          <article className="history-stat">
            <span>Baseline pending</span>
            <strong>{data?.summary.pendingCount ?? 0}</strong>
          </article>
          <article className="history-stat">
            <span>Slot changes</span>
            <strong>{data?.summary.slotChangeCount ?? 0}</strong>
          </article>
        </div>
      </section>

      <section className="panel">
        <div className="section-header">
          <div>
            <p className="section-kicker">Changes</p>
            <h2>Flagged detections</h2>
          </div>
          <p className="section-copy">
            These are the strongest percent changes for the selected interval.
          </p>
        </div>
        <div className="insight-grid">
          {topChanges.map((row) => (
            <article key={`${row.signalId}-${row.label}`} className="insight-card">
              <div className="insight-top">
                <div className="insight-heading">
                  <span className="insight-label">{row.signalId}</span>
                </div>
                <strong className="insight-value">{row.label}</strong>
              </div>
              <p className="bottleneck-cluster-tag">{row.signalName}</p>
              <p className="bottleneck-cluster-summary">
                {selectedMoeLabel}: {row.value?.toFixed(2) ?? "—"} vs historical mean {row.historicalMean?.toFixed(2) ?? "—"}
              </p>
              <div className="day-chip-row">
                <span className="day-chip bottleneck-summary-chip active">
                  {row.direction === "above" ? "Higher than history" : row.direction === "below" ? "Lower than history" : "Near history"}
                </span>
                <span className="day-chip bottleneck-summary-chip active">History {row.historyCount}</span>
                <span className="day-chip bottleneck-summary-chip active">Delta {row.deltaPct?.toFixed(1) ?? "—"}%</span>
              </div>
            </article>
          ))}
          {topChanges.length ? null : (
            <article className="insight-card">
              <div className="insight-top">
                <div className="insight-heading">
                  <span className="insight-label">No changes flagged</span>
                </div>
              </div>
              <p>The selected interval is staying close to the historical mean for the current intersection selection and MOE.</p>
            </article>
          )}
        </div>
      </section>

      <section className="table-panel">
        <div className="section-header">
          <div>
            <p className="section-kicker">Observations</p>
            <h2>Latest week checks</h2>
          </div>
          <p className="section-copy">
            Each row shows the selected {selectedMoeLabel} value against the historical mean for that same intersection.
          </p>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Signal</th>
                <th>Intersection</th>
                <th>Period</th>
                <th>Value</th>
                <th>Historical Mean</th>
                <th>History Count</th>
                <th>Delta %</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {slotRows.map((row) => (
                <tr key={`${row.signalId}-${row.label}`}>
                  <td>{row.signalId}</td>
                  <td>{row.signalName}</td>
                  <td>{row.label}</td>
                  <td>{row.value?.toFixed(2) ?? "—"}</td>
                  <td>{row.historicalMean?.toFixed(2) ?? "—"}</td>
                  <td>{row.historyCount}</td>
                  <td>{row.deltaPct?.toFixed(1) ?? "—"}%</td>
                  <td>
                    <span className={`change-status change-status--${row.tone === "alert" ? "change" : row.historicalMean === null ? "pending" : "stable"}`}>
                      {row.status}
                    </span>
                  </td>
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
  const [historicalDashboard, setHistoricalDashboard] = useState<DashboardResponse | null>(null);
  const [activeView, setActiveView] = useState<"dashboard" | "historicalDashboard" | "historical" | "ranking" | "dayHour" | "bottleneck" | "hdbscan" | "changeDetection">("dashboard");
  const [historical, setHistorical] = useState<HistoricalRunResponse | null>(null);
  const [rankingData, setRankingData] = useState<RankingResponse | null>(null);
  const [dayHourData, setDayHourData] = useState<DayHourResponse | null>(null);
  const [bottleneckData, setBottleneckData] = useState<BottleneckResponse | null>(null);
  const [hdbscanData, setHdbscanData] = useState<HdbscanResponse | null>(null);
  const [changeDetectionData, setChangeDetectionData] = useState<ChangeDetectionResponse | null>(null);
  const [historicalSignalId, setHistoricalSignalId] = useState("1470");
  const [dayHourMetric, setDayHourMetric] = useState<Exclude<RankingMetricKey, "compositeScore">>("avgWait");
  const [bottleneckSignalId, setBottleneckSignalId] = useState("1240");
  const [bottleneckPhase, setBottleneckPhase] = useState("1");
  const [bottleneckMoes, setBottleneckMoes] = useState<string[]>(["aogPct", "aorPct", "volume", "avgWait", "splitFailures"]);
  const [bottleneckEps, setBottleneckEps] = useState("0.25");
  const [bottleneckMinSamples, setBottleneckMinSamples] = useState("2");
  const [hdbscanSignalIds, setHdbscanSignalIds] = useState<string[]>(["1240", "1242", "1244"]);
  const [hdbscanPhases, setHdbscanPhases] = useState<string[]>(["1"]);
  const [hdbscanMoes, setHdbscanMoes] = useState<string[]>(["aogPct", "aorPct", "volume", "avgWait", "splitFailures"]);
  const [hdbscanMinClusterSize, setHdbscanMinClusterSize] = useState("6");
  const [hdbscanMinSamples, setHdbscanMinSamples] = useState("3");
  const [hdbscanClusterSelectionEpsilon, setHdbscanClusterSelectionEpsilon] = useState("0.05");
  const [changeDetectionSignalIds, setChangeDetectionSignalIds] = useState<string[]>(["1240", "1242", "1244"]);
  const [changeDetectionMoe, setChangeDetectionMoe] = useState<Exclude<RankingMetricKey, "compositeScore">>("avgWait");
  const [changeDetectionGranularity, setChangeDetectionGranularity] = useState<"day" | "hour">("hour");
  const [changeDetectionSelectedDate, setChangeDetectionSelectedDate] = useState("2026-03-04");
  const [changeDetectionSelectedHour, setChangeDetectionSelectedHour] = useState("06:00");
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
  const [historicalDashboardIntersectionIds, setHistoricalDashboardIntersectionIds] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [historicalDashboardLoading, setHistoricalDashboardLoading] = useState(false);
  const [historicalLoading, setHistoricalLoading] = useState(false);
  const [rankingLoading, setRankingLoading] = useState(false);
  const [dayHourLoading, setDayHourLoading] = useState(false);
  const [bottleneckLoading, setBottleneckLoading] = useState(false);
  const [hdbscanLoading, setHdbscanLoading] = useState(false);
  const [changeDetectionLoading, setChangeDetectionLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [historicalDashboardError, setHistoricalDashboardError] = useState<string | null>(null);
  const [historicalError, setHistoricalError] = useState<string | null>(null);
  const [rankingError, setRankingError] = useState<string | null>(null);
  const [dayHourError, setDayHourError] = useState<string | null>(null);
  const [bottleneckError, setBottleneckError] = useState<string | null>(null);
  const [hdbscanError, setHdbscanError] = useState<string | null>(null);
  const [changeDetectionError, setChangeDetectionError] = useState<string | null>(null);

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

  const buildHistoricalDashboardParams = (intersectionIdsOverride?: string[]) =>
    new URLSearchParams({
      dateFrom: filters.dateFrom,
      dateTo: filters.dateTo,
      daysOfWeek: filters.daysOfWeek.join(","),
      hourFrom: filters.hourFrom,
      hourTo: filters.hourTo,
      timeOfDayPreset: filters.timeOfDayPreset,
      phase: filters.selectedPhase,
      intersectionIds: (intersectionIdsOverride ?? historicalDashboardIntersectionIds).join(",")
    });

  useEffect(() => {
    const controller = new AbortController();

    async function loadDashboard() {
      setLoading(true);
      setError(null);
      const params = buildDashboardParams();

      try {
        const response = await fetch(`${API_BASE_URL}/api/dashboard?${params.toString()}`, {
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
    if (activeView !== "historicalDashboard") {
      return;
    }
    const controller = new AbortController();

    async function loadHistoricalDashboard() {
      setHistoricalDashboardLoading(true);
      setHistoricalDashboardError(null);
      const params = buildHistoricalDashboardParams();

      try {
        const response = await fetch(`${API_BASE_URL}/api/historical-dashboard?${params.toString()}`, {
          signal: controller.signal
        });
        if (!response.ok) {
          throw new Error(`Backend request failed with status ${response.status}`);
        }
        const nextDashboard: DashboardResponse = await response.json();
        setHistoricalDashboard(nextDashboard);
        setHistoricalDashboardIntersectionIds(nextDashboard.filters.selectedIntersectionIds);
      } catch (requestError) {
        if ((requestError as Error).name !== "AbortError") {
          setHistoricalDashboardError((requestError as Error).message);
        }
      } finally {
        setHistoricalDashboardLoading(false);
      }
    }

    loadHistoricalDashboard();
    return () => controller.abort();
  }, [
    activeView,
    filters.dateFrom,
    filters.dateTo,
    filters.daysOfWeek.join(","),
    filters.hourFrom,
    filters.hourTo,
    filters.timeOfDayPreset,
    filters.selectedPhase,
    historicalDashboardIntersectionIds.join(",")
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
        const response = await fetch(`${API_BASE_URL}/api/ranking?${params.toString()}`, {
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
        const response = await fetch(`${API_BASE_URL}/api/historical-run?${params.toString()}`, {
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
        const response = await fetch(`${API_BASE_URL}/api/day-hour?${params.toString()}`, {
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

  useEffect(() => {
    if (activeView !== "bottleneck") {
      return;
    }
    const controller = new AbortController();

    async function loadBottleneckView() {
      setBottleneckLoading(true);
      setBottleneckError(null);
      try {
        const params = new URLSearchParams({
          signalId: bottleneckSignalId,
          phase: bottleneckPhase,
          moes: bottleneckMoes.join(","),
          eps: bottleneckEps,
          minSamples: bottleneckMinSamples,
        });
        const response = await fetch(`${API_BASE_URL}/api/bottleneck-clusters?${params.toString()}`, {
          signal: controller.signal,
        });
        if (!response.ok) {
          throw new Error(`Backend request failed with status ${response.status}`);
        }
        const nextData: BottleneckResponse = await response.json();
        setBottleneckData(nextData);
        setBottleneckSignalId(nextData.signal.id);
        setBottleneckPhase(String(nextData.filters.phase));
        setBottleneckEps(String(nextData.filters.eps));
        setBottleneckMinSamples(String(nextData.filters.minSamples));
      } catch (requestError) {
        if ((requestError as Error).name !== "AbortError") {
          setBottleneckError((requestError as Error).message);
        }
      } finally {
        setBottleneckLoading(false);
      }
    }

    loadBottleneckView();
    return () => controller.abort();
  }, [activeView, bottleneckSignalId, bottleneckPhase, bottleneckMoes.join(","), bottleneckEps, bottleneckMinSamples]);

  useEffect(() => {
    if (activeView !== "hdbscan") {
      return;
    }
    const controller = new AbortController();

    async function loadHdbscanView() {
      setHdbscanLoading(true);
      setHdbscanError(null);
      try {
        const params = new URLSearchParams({
          signalIds: hdbscanSignalIds.join(","),
          phases: hdbscanPhases.join(","),
          moes: hdbscanMoes.join(","),
          minClusterSize: hdbscanMinClusterSize,
          minSamples: hdbscanMinSamples,
          clusterSelectionEpsilon: hdbscanClusterSelectionEpsilon,
        });
        const response = await fetch(`${API_BASE_URL}/api/hdbscan-clusters?${params.toString()}`, {
          signal: controller.signal,
        });
        if (!response.ok) {
          throw new Error(`Backend request failed with status ${response.status}`);
        }
        const nextData: HdbscanResponse = await response.json();
        setHdbscanData(nextData);
        setHdbscanSignalIds(nextData.filters.signalIds);
        setHdbscanPhases(nextData.filters.phases.map(String));
        setHdbscanMinClusterSize(String(nextData.filters.minClusterSize));
        setHdbscanMinSamples(String(nextData.filters.minSamples));
        setHdbscanClusterSelectionEpsilon(String(nextData.filters.clusterSelectionEpsilon));
      } catch (requestError) {
        if ((requestError as Error).name !== "AbortError") {
          setHdbscanError((requestError as Error).message);
        }
      } finally {
        setHdbscanLoading(false);
      }
    }

    loadHdbscanView();
    return () => controller.abort();
  }, [
    activeView,
    hdbscanSignalIds.join(","),
    hdbscanPhases.join(","),
    hdbscanMoes.join(","),
    hdbscanMinClusterSize,
    hdbscanMinSamples,
    hdbscanClusterSelectionEpsilon,
  ]);

  useEffect(() => {
    if (activeView !== "changeDetection") {
      return;
    }
    const controller = new AbortController();

    async function loadChangeDetectionView() {
      setChangeDetectionLoading(true);
      setChangeDetectionError(null);

      try {
        const params = new URLSearchParams({
          signalIds: changeDetectionSignalIds.join(","),
          moe: changeDetectionMoe,
          granularity: changeDetectionGranularity,
          selectedDate: changeDetectionSelectedDate,
          selectedHour: changeDetectionSelectedHour,
        });
        const response = await fetch(`${API_BASE_URL}/api/change-detection?${params.toString()}`, {
          signal: controller.signal
        });
        if (!response.ok) {
          throw new Error(`Backend request failed with status ${response.status}`);
        }
        const nextData: ChangeDetectionResponse = await response.json();
        setChangeDetectionData(nextData);
        setChangeDetectionSignalIds(nextData.filters.signalIds);
        setChangeDetectionMoe(nextData.filters.moe);
        setChangeDetectionGranularity(nextData.filters.granularity);
        setChangeDetectionSelectedDate(nextData.filters.selectedDate);
        setChangeDetectionSelectedHour(nextData.filters.selectedHour);
      } catch (requestError) {
        if ((requestError as Error).name !== "AbortError") {
          setChangeDetectionError((requestError as Error).message);
        }
      } finally {
        setChangeDetectionLoading(false);
      }
    }

    loadChangeDetectionView();
    return () => controller.abort();
  }, [
    activeView,
    changeDetectionSignalIds.join(","),
    changeDetectionMoe,
    changeDetectionGranularity,
    changeDetectionSelectedDate,
    changeDetectionSelectedHour,
  ]);

  const isHistoricalDashboardView = activeView === "historicalDashboard";
  const currentDashboard = isHistoricalDashboardView ? historicalDashboard : dashboard;
  const currentSelectedIntersectionIds = isHistoricalDashboardView ? historicalDashboardIntersectionIds : selectedIntersectionIds;
  const currentDashboardLoading = isHistoricalDashboardView ? historicalDashboardLoading : loading;
  const currentDashboardError = isHistoricalDashboardView ? historicalDashboardError : error;

  const corridor = currentDashboard?.corridor;
  const intersections = corridor?.intersections ?? [];
  const allIntersections = corridor?.allIntersections ?? [];
  const selectedIntersection = intersections[0];

  const toggleIntersectionSelection = (intersectionId: string) => {
    const setIds = isHistoricalDashboardView ? setHistoricalDashboardIntersectionIds : setSelectedIntersectionIds;
    setIds((current) => {
      const active = current.includes(intersectionId);
      if (active && current.length === 1) {
        return current;
      }
      return active ? current.filter((id) => id !== intersectionId) : [...current, intersectionId];
    });
  };

  const toggleHdbscanSignalSelection = (intersectionId: string) => {
    setHdbscanSignalIds((current) => {
      const active = current.includes(intersectionId);
      if (active && current.length === 1) {
        return current;
      }
      const next = active ? current.filter((id) => id !== intersectionId) : [...current, intersectionId];
      const order = (hdbscanData?.meta.availableSignals ?? []).map((signal) => signal.id);
      return order.filter((id) => next.includes(id));
    });
  };

  const toggleHdbscanPhase = (phase: string) => {
    setHdbscanPhases((current) => {
      const active = current.includes(phase);
      if (active && current.length === 1) {
        return current;
      }
      const next = active ? current.filter((item) => item !== phase) : [...current, phase];
      return [...next].sort((left, right) => Number(left) - Number(right));
    });
  };

  const toggleHdbscanMoe = (key: string) => {
    setHdbscanMoes((current) => {
      const active = current.includes(key);
      if (active && current.length === 1) {
        return current;
      }
      return active ? current.filter((item) => item !== key) : [...current, key];
    });
  };

  const toggleChangeDetectionSignalSelection = (intersectionId: string) => {
    setChangeDetectionSignalIds((current) => {
      const active = current.includes(intersectionId);
      if (active && current.length === 1) {
        return current;
      }
      const next = active ? current.filter((id) => id !== intersectionId) : [...current, intersectionId];
      const order = (changeDetectionData?.meta.availableSignals ?? []).map((signal) => signal.id);
      return order.filter((id) => next.includes(id));
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

  const corridorOrderedIntersections = useMemo<RankedIntersection[]>(() => {
    const rankedById = new Map(rankedIntersections.map((intersection) => [intersection.id, intersection]));
    return [...allIntersections]
      .sort((left, right) => Number(left.id) - Number(right.id))
      .map((intersection) => rankedById.get(intersection.id) ?? {
        ...intersection,
        rank: null,
        score: null,
        category: "nodata" as const
      });
  }, [allIntersections, rankedIntersections]);

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

  const liveWorstWaitData = useMemo(
    () =>
      [...intersections]
        .map((intersection) => ({
          name: intersection.name,
          avgWait: intersection.summary.meanDelay,
          aog: intersection.summary.meanAog
        }))
        .sort((left, right) => right.avgWait - left.avgWait)
        .slice(0, 8),
    [intersections]
  );

  const historicalComparisonData = useMemo(
    () =>
      intersections.map((intersection) => ({
        name: intersection.name,
        avgWait: intersection.summary.meanDelay,
        splitFailures: intersection.summary.splitFailures
      })),
    [intersections]
  );

  const glossaryCards = useMemo(
    () =>
      Object.entries(currentDashboard?.meta.metricDefinitions ?? {}).map(([key, meaning]) => ({
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
    [currentDashboard?.meta.metricDefinitions]
  );

  const selectedPreset = currentDashboard?.meta.timeOfDayPresets.find((preset) => preset.id === filters.timeOfDayPreset);
  const timeOfDayPresets = currentDashboard?.meta.timeOfDayPresets.length
    ? currentDashboard.meta.timeOfDayPresets
    : fallbackTimeOfDayPresets;
  const availableDays = currentDashboard?.meta.availableDays.length ? currentDashboard.meta.availableDays : fallbackDays;
  const availableDates = currentDashboard?.meta.availableDates ?? [];
  const maxAvailableDate = availableDates.length ? availableDates[availableDates.length - 1] : undefined;

  return (
    <div className="app-shell">
      <header className="topbar">
        <div>
          <h1>Bee Ridge ATSPM Dashboard</h1>
        </div>
      </header>

      <nav className="view-tabs" aria-label="Dashboard views">
        <button
          type="button"
          className={activeView === "historicalDashboard" ? "active" : ""}
          onClick={() => setActiveView("historicalDashboard")}
        >
          Dashboard
        </button>
        <button
          type="button"
          className={activeView === "historical" ? "active" : ""}
          onClick={() => setActiveView("historical")}
        >
          Intersection-wise Analysis
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
        <button
          type="button"
          className={activeView === "bottleneck" ? "active" : ""}
          onClick={() => setActiveView("bottleneck")}
        >
          Bottleneck Clusters
        </button>
        <button
          type="button"
          className={activeView === "hdbscan" ? "active" : ""}
          onClick={() => setActiveView("hdbscan")}
        >
          HDBSCAN Clusters
        </button>
        <button
          type="button"
          className={activeView === "changeDetection" ? "active" : ""}
          onClick={() => setActiveView("changeDetection")}
        >
          Change Detection
        </button>
        <button
          type="button"
          className={activeView === "dashboard" ? "active" : ""}
          onClick={() => setActiveView("dashboard")}
        >
          Ad Hoc Queries
        </button>
      </nav>

      {currentDashboard?.meta.note ? <div className="status-banner">{currentDashboard.meta.note}</div> : null}
      {currentDashboardError ? <div className="status-banner error">{currentDashboardError}</div> : null}
      {activeView === "dashboard" ? (
        <div className="status-banner live-dashboard-note">
          These fine-grained queries may take several seconds to minutes in order to execute.
        </div>
      ) : null}
      {currentDashboardLoading && (activeView === "dashboard" || activeView === "historicalDashboard") ? (
        <div className="status-banner">
          {activeView === "historicalDashboard"
            ? "Loading the precomputed dashboard..."
            : "Loading ATSPM aggregates from the selected October files..."}
        </div>
      ) : null}

      {activeView === "dashboard" || activeView === "historicalDashboard" ? (
      <main className="workspace">
        <section className="map-panel">
          <div className="section-header">
            <div>
              <p className="section-kicker">Map</p>
              <h2>{corridor?.name ?? "Florida ATSPM map"}</h2>
            </div>
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
              {corridorOrderedIntersections.map((intersection) => {
                const active = currentSelectedIntersectionIds.includes(intersection.id);
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
                {currentDashboardLoading
                  ? activeView === "historicalDashboard"
                    ? "Loading mapped signals from the precomputed MOE outputs..."
                    : "Loading mapped signals from the October ATSPM files..."
                  : `${corridorOrderedIntersections.length} mapped signals shown in corridor order.`}
              </p>
              <div className="preset-row">
                {(currentDashboard?.meta.selectionPresets ?? []).map((preset) => (
                  <button
                    key={preset.id}
                    type="button"
                    className="preset-chip"
                    onClick={() =>
                      (isHistoricalDashboardView ? setHistoricalDashboardIntersectionIds : setSelectedIntersectionIds)(
                        preset.intersectionIds.filter((id) => allIntersections.some((item) => item.id === id))
                      )
                    }
                  >
                    {preset.label}
                  </button>
                ))}
              </div>
              {corridorOrderedIntersections.map((intersection) => {
                const active = currentSelectedIntersectionIds.includes(intersection.id);
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
            <p className="section-copy">Use time-of-day presets or a custom time range, then narrow to the days you want inside the date window.</p>
          </div>

          <div className="filters-grid filters-grid-v1">
            <label>
              <span>Time of day</span>
              <CustomSelect
                value={filters.timeOfDayPreset}
                options={timeOfDayPresets.map((preset) => ({ value: preset.id, label: preset.label }))}
                onChange={(nextValue) => {
                  const nextPreset = timeOfDayPresets.find((item) => item.id === nextValue);
                  setFilters((current) => ({
                    ...current,
                    timeOfDayPreset: nextValue,
                    hourFrom: nextPreset?.start ?? current.hourFrom,
                    hourTo: nextPreset?.end ?? current.hourTo
                  }));
                }}
                panelClassName="custom-select-panel--tall"
              />
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
              <span>{activeView === "dashboard" ? "Time from" : "Hour from"}</span>
              {activeView === "dashboard" ? (
                <input
                  type="time"
                  step={60}
                  value={filters.hourFrom}
                  onChange={(event) =>
                    setFilters((current) => ({ ...current, hourFrom: event.target.value, timeOfDayPreset: "custom" }))
                  }
                />
              ) : (
                <CustomSelect
                  value={filters.hourFrom}
                  options={hourOptions.map((hour) => ({ value: hour, label: hour }))}
                  onChange={(nextValue) => setFilters((current) => ({ ...current, hourFrom: nextValue, timeOfDayPreset: "custom" }))}
                  panelClassName="custom-select-panel--hours"
                />
              )}
            </label>
            <label>
              <span>{activeView === "dashboard" ? "Time to" : "Hour to"}</span>
              {activeView === "dashboard" ? (
                <input
                  type="time"
                  step={60}
                  value={filters.hourTo}
                  onChange={(event) =>
                    setFilters((current) => ({ ...current, hourTo: event.target.value, timeOfDayPreset: "custom" }))
                  }
                />
              ) : (
                <CustomSelect
                  value={filters.hourTo}
                  options={hourOptions.map((hour) => ({ value: hour, label: hour }))}
                  onChange={(nextValue) => setFilters((current) => ({ ...current, hourTo: nextValue, timeOfDayPreset: "custom" }))}
                  panelClassName="custom-select-panel--hours"
                />
              )}
            </label>
              <div className="filter-summary">
                <p className="filter-summary-label">Scope</p>
                <p>{selectedPreset?.label ?? "Custom Period"}</p>
                <strong>
                  {currentSelectedIntersectionIds.length} signals, {filters.hourFrom} to {filters.hourTo}
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
            <p className="section-copy">Each metric includes a quick tooltip definition.</p>
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
              <span className="filter-summary-label">Phase selected</span>
              <div className="day-chip-row phase-chip-row" role="tablist" aria-label="Phase selector">
                <button
                  type="button"
                  className={`day-chip phase-chip ${filters.selectedPhase === "all" ? "active" : ""}`}
                  onClick={() => setFilters((current) => ({ ...current, selectedPhase: "all" }))}
                >
                  All
                </button>
                {(corridor?.availablePhases ?? []).map((phase) => {
                  const value = String(phase);
                  return (
                    <button
                      key={phase}
                      type="button"
                      className={`day-chip phase-chip ${filters.selectedPhase === value ? "active" : ""}`}
                      onClick={() => setFilters((current) => ({ ...current, selectedPhase: value }))}
                    >
                      P{phase}
                    </button>
                  );
                })}
              </div>
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
                  <XAxis
                    dataKey="name"
                    stroke="#4c5a57"
                    interval={0}
                    height={88}
                    angle={-24}
                    textAnchor="end"
                    tick={{ fontSize: 11 }}
                  />
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
              <p className="section-kicker">{activeView === "historicalDashboard" ? "Comparison" : "Priority list"}</p>
              <h2>
                {activeView === "historicalDashboard"
                  ? "Intersection comparison by wait and split failures"
                  : "Top worst selected intersections by average wait"}
              </h2>
            </div>
          </div>
          <div className="chart-wrap">
            <ResponsiveContainer width="100%" height={300}>
              {activeView === "historicalDashboard" ? (
                <BarChart data={historicalComparisonData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#d9ddd5" />
                  <XAxis
                    dataKey="name"
                    stroke="#4c5a57"
                    interval={0}
                    height={92}
                    angle={-24}
                    textAnchor="end"
                    tick={{ fontSize: 11 }}
                  />
                  <YAxis yAxisId="left" stroke="#4c5a57" />
                  <YAxis yAxisId="right" orientation="right" stroke="#4c5a57" />
                  <Tooltip />
                  <Legend />
                  <Bar yAxisId="left" dataKey="avgWait" name="Avg Wait (s)" fill="#c98358" radius={[6, 6, 0, 0]} />
                  <Bar yAxisId="right" dataKey="splitFailures" name="Split Failures" fill="#5b8f69" radius={[6, 6, 0, 0]} />
                </BarChart>
              ) : (
                <BarChart data={liveWorstWaitData} layout="vertical" margin={{ left: 48, right: 16 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#d9ddd5" />
                  <XAxis type="number" stroke="#4c5a57" />
                  <YAxis type="category" dataKey="name" stroke="#4c5a57" width={200} tick={{ fontSize: 11 }} />
                  <Tooltip />
                  <Bar dataKey="avgWait" name="Avg Wait (s)" fill="#c98358" radius={[0, 6, 6, 0]} />
                </BarChart>
              )}
            </ResponsiveContainer>
          </div>
          {activeView === "historicalDashboard"
            ? insightBlock(
                "What this shows",
                "A side-by-side comparison of selected intersections using average wait and split failures from the precomputed MOE outputs.",
                historicalComparisonData.length
                  ? "This shows which selected intersections combine higher waiting pressure with higher split-failure pressure in the selected historical window."
                  : "No historical comparison data is available for the current selection."
              )
            : insightBlock(
                "What this shows",
                "The selected intersections ordered by highest average wait.",
                liveWorstWaitData.length
                  ? "This immediately shows which selected intersections are carrying the most waiting pressure in the current live filter."
                  : "No selected-intersection wait data is available for the current selection."
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
            </div>

            <div className="filters-grid filters-grid-v1">
              <label>
                <span>Time of day</span>
                <CustomSelect
                  value={filters.timeOfDayPreset}
                  options={timeOfDayPresets.map((preset) => ({ value: preset.id, label: preset.label }))}
                  onChange={(nextValue) => {
                    const nextPreset = timeOfDayPresets.find((item) => item.id === nextValue);
                    setFilters((current) => ({
                      ...current,
                      timeOfDayPreset: nextValue,
                      hourFrom: nextPreset?.start ?? current.hourFrom,
                      hourTo: nextPreset?.end ?? current.hourTo
                    }));
                  }}
                  panelClassName="custom-select-panel--tall"
                />
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
                <CustomSelect
                  value={filters.hourFrom}
                  options={hourOptions.map((hour) => ({ value: hour, label: hour }))}
                  onChange={(nextValue) => setFilters((current) => ({ ...current, hourFrom: nextValue, timeOfDayPreset: "custom" }))}
                  panelClassName="custom-select-panel--hours"
                />
              </label>
              <label>
                <span>Hour to</span>
                <CustomSelect
                  value={filters.hourTo}
                  options={hourOptions.map((hour) => ({ value: hour, label: hour }))}
                  onChange={(nextValue) => setFilters((current) => ({ ...current, hourTo: nextValue, timeOfDayPreset: "custom" }))}
                  panelClassName="custom-select-panel--hours"
                />
              </label>
              <div className="filter-summary">
                <p className="filter-summary-label">Scope</p>
                <p>Precomputed MOE ranking</p>
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
            intersections={dashboard?.corridor.allIntersections ?? historicalDashboard?.corridor.allIntersections ?? []}
          />
        </main>
      ) : activeView === "bottleneck" ? (
        <BottleneckClusterView
          data={bottleneckData}
          loading={bottleneckLoading}
          error={bottleneckError}
          selectedSignalId={bottleneckSignalId}
          onSignalChange={setBottleneckSignalId}
          selectedPhase={bottleneckPhase}
          onPhaseChange={setBottleneckPhase}
          selectedMoes={bottleneckMoes}
          eps={bottleneckEps}
          onEpsChange={setBottleneckEps}
          minSamples={bottleneckMinSamples}
          onMinSamplesChange={setBottleneckMinSamples}
          onMoeToggle={(value) =>
            setBottleneckMoes((current) => {
              const active = current.includes(value);
              if (active && current.length === 1) {
                return current;
              }
              return active ? current.filter((item) => item !== value) : [...current, value];
            })
          }
        />
      ) : activeView === "hdbscan" ? (
        <HdbscanClusterView
          data={hdbscanData}
          loading={hdbscanLoading}
          error={hdbscanError}
          selectedSignalIds={hdbscanSignalIds}
          onToggleSignal={toggleHdbscanSignalSelection}
          onApplySignalPreset={setHdbscanSignalIds}
          selectedPhases={hdbscanPhases}
          onTogglePhase={toggleHdbscanPhase}
          selectedMoes={hdbscanMoes}
          onMoeToggle={toggleHdbscanMoe}
          minClusterSize={hdbscanMinClusterSize}
          onMinClusterSizeChange={setHdbscanMinClusterSize}
          minSamples={hdbscanMinSamples}
          onMinSamplesChange={setHdbscanMinSamples}
          clusterSelectionEpsilon={hdbscanClusterSelectionEpsilon}
          onClusterSelectionEpsilonChange={setHdbscanClusterSelectionEpsilon}
        />
      ) : activeView === "changeDetection" ? (
        <ChangeDetectionView
          data={changeDetectionData}
          loading={changeDetectionLoading}
          error={changeDetectionError}
          selectedSignalIds={changeDetectionSignalIds}
          onToggleSignal={toggleChangeDetectionSignalSelection}
          onApplySignalPreset={setChangeDetectionSignalIds}
          selectedMoe={changeDetectionMoe}
          onMoeChange={setChangeDetectionMoe}
          granularity={changeDetectionGranularity}
          onGranularityChange={setChangeDetectionGranularity}
          selectedDate={changeDetectionSelectedDate}
          onSelectedDateChange={setChangeDetectionSelectedDate}
          selectedHour={changeDetectionSelectedHour}
          onSelectedHourChange={setChangeDetectionSelectedHour}
        />
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
