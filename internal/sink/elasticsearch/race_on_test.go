//go:build race

package elasticsearch

// raceDetectorEnabled reports that this binary was built with -race.
//
// The detector instruments every memory access and allocation, multiplying both
// time and allocation counts several fold. Performance budgets measured under it
// describe the instrumentation rather than the code, so they are skipped.
const raceDetectorEnabled = true
