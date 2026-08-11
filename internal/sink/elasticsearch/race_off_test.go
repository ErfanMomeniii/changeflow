//go:build !race

package elasticsearch

// raceDetectorEnabled reports that this binary was built without -race.
const raceDetectorEnabled = false
