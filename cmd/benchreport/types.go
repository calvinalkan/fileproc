package main

// Summary is the per-run summary record stored in history.jsonl.
type Summary struct {
	Timestamp string `json:"ts"`
	RunDir    string `json:"run_dir"`

	Git    GitInfo    `json:"git"`
	Env    EnvInfo    `json:"env"`
	Config ConfigInfo `json:"config"`

	// Results maps benchmark name (e.g. "flat_1k_bytes") to its metrics.
	Results map[string]BenchResult `json:"results"`
}

type GitInfo struct {
	Rev       string `json:"rev"`
	Dirty     bool   `json:"dirty"`
	UserName  string `json:"user_name"`
	UserEmail string `json:"user_email"`
}

type EnvInfo struct {
	Hostname       string `json:"hostname"`
	CPUModel       string `json:"cpu_model"`
	GoVersion      string `json:"go_version"`
	PowerSource    string `json:"power_source"`
	CPUGovernor    string `json:"cpu_governor"`
	EnergyPerfPref string `json:"energy_perf_pref"`
}

type ConfigInfo struct {
	Workers string `json:"workers"`
	GC      string `json:"gc"`
	Read    string `json:"read"`
	CPUSet  string `json:"cpu_set"`
}

type BenchResult struct {
	MeanMs      float64 `json:"mean_ms"`
	StddevMs    float64 `json:"stddev_ms"`
	MedianMs    float64 `json:"median_ms"`
	MinMs       float64 `json:"min_ms"`
	MaxMs       float64 `json:"max_ms"`
	FilesPerSec float64 `json:"files_per_sec"`
	BytesPerSec float64 `json:"bytes_per_sec,omitempty"`
	MBPerSec    float64 `json:"mb_per_sec,omitempty"`
	Runs        int     `json:"runs"`
}

// HyperfineOutput is the JSON structure produced by hyperfine --export-json.
type HyperfineOutput struct {
	Results []HyperfineResult `json:"results"`
}

type HyperfineResult struct {
	Command string    `json:"command"`
	Mean    float64   `json:"mean"`   // seconds
	Stddev  float64   `json:"stddev"` // seconds
	Median  float64   `json:"median"` // seconds
	Min     float64   `json:"min"`    // seconds
	Max     float64   `json:"max"`    // seconds
	Times   []float64 `json:"times"`  // all samples in seconds
}
