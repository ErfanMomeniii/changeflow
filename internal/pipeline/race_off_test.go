//go:build !race

package pipeline

// raceDetectorEnabled reports that this binary was built without -race.
const raceDetectorEnabled = false
