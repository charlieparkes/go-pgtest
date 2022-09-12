package pgtest

import "go.uber.org/zap"

type Opt func(*Postgres)

func OptName(name string) Opt {
	return func(f *Postgres) {
		f.name = name
	}
}

func OptSettings(settings *ConnectionSettings) Opt {
	return func(f *Postgres) {
		f.settings = settings
	}
}

func OptRepo(repo string) Opt {
	return func(f *Postgres) {
		f.repo = repo
	}
}

func OptVersion(version string) Opt {
	return func(f *Postgres) {
		f.version = version
	}
}

// Tell docker to kill the container after an unreasonable amount of test time to prevent orphans. Defaults to 600 seconds.
func OptExpireAfter(expireAfter uint) Opt {
	return func(f *Postgres) {
		f.expireAfter = expireAfter
	}
}

// Wait this long for operations to execute. Defaults to 30 seconds.
func OptTimeoutAfter(timeoutAfter uint) Opt {
	return func(f *Postgres) {
		f.timeoutAfter = timeoutAfter
	}
}

func OptSkipTearDown() Opt {
	return func(f *Postgres) {
		f.skipTearDown = true
	}
}

func OptMounts(mounts []string) Opt {
	return func(f *Postgres) {
		f.mounts = mounts
	}
}

func OptLogger(logger *zap.Logger) Opt {
	return func(f *Postgres) {
		f.log = logger
	}
}

func OptNetworkName(networkName string) Opt {
	return func(f *Postgres) {
		f.networkName = networkName
	}
}
