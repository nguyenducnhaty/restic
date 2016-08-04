package main

import (
	"restic"
	"restic/backend"
	"restic/debug"
	"restic/pack"
	"restic/repository"
	"restic/worker"

	"github.com/cheggaaa/pb"
)

// CmdPrune implements the 'prune' command.
type CmdPrune struct {
	global *GlobalOptions
}

func init() {
	_, err := parser.AddCommand("prune",
		"removes content from a repository",
		"The prune command removes rendundant and unneeded data from the repository",
		&CmdPrune{global: &globalOpts})
	if err != nil {
		panic(err)
	}
}

// Execute runs the 'prune' command.
func (cmd CmdPrune) Execute(args []string) error {
	repo, err := cmd.global.OpenRepository()
	if err != nil {
		return err
	}

	lock, err := lockRepoExclusive(repo)
	defer unlockRepo(lock)
	if err != nil {
		return err
	}

	err = repo.LoadIndex()
	if err != nil {
		return err
	}

	done := make(chan struct{})
	defer close(done)

	cmd.global.Verbosef("loading list of files from the repo\n")

	var stats struct {
		blobs     int
		packs     int
		snapshots int
	}

	packs := make(map[backend.ID]pack.BlobSet)
	for packID := range repo.List(backend.Data, done) {
		debug.Log("CmdPrune.Execute", "found %v", packID.Str())
		packs[packID] = pack.NewBlobSet()
		stats.packs++
	}

	cmd.global.Verbosef("listing %v files\n", stats.packs)

	blobCount := make(map[backend.ID]int)
	duplicateBlobs := 0
	duplicateBytes := 0
	rewritePacks := backend.NewIDSet()

	ch := make(chan worker.Job)
	go repository.ListAllPacks(repo, ch, done)

	for job := range ch {
		packID := job.Data.(backend.ID)
		if job.Error != nil {
			cmd.global.Warnf("unable to list pack %v: %v\n", packID.Str(), job.Error)
			continue
		}

		j := job.Result.(repository.ListAllPacksResult)

		debug.Log("CmdPrune.Execute", "pack %v contains %d blobs", packID.Str(), len(j.Entries))
		for _, pb := range j.Entries {
			packs[packID].Insert(pack.Handle{ID: pb.ID, Type: pb.Type})
			stats.blobs++
			blobCount[pb.ID]++

			if blobCount[pb.ID] > 1 {
				duplicateBlobs++
				duplicateBytes += int(pb.Length)
			}
		}
	}

	cmd.global.Verbosef("processed %d blobs: %d duplicate blobs, %d duplicate bytes\n",
		stats.blobs, duplicateBlobs, duplicateBytes)
	cmd.global.Verbosef("load all snapshots\n")

	snapshots, err := restic.LoadAllSnapshots(repo)
	if err != nil {
		return err
	}

	stats.snapshots = len(snapshots)

	cmd.global.Verbosef("find data that is still in use for %d snapshots\n", stats.snapshots)

	usedBlobs := pack.NewBlobSet()
	seenBlobs := pack.NewBlobSet()
	pb := pb.New(len(snapshots))
	pb.Width = 70
	pb.Start()
	for _, sn := range snapshots {
		debug.Log("CmdPrune.Execute", "process snapshot %v", sn.ID().Str())

		err = restic.FindUsedBlobs(repo, *sn.Tree, usedBlobs, seenBlobs)
		if err != nil {
			return err
		}

		debug.Log("CmdPrune.Execute", "found %v blobs for snapshot %v", sn.ID().Str())
		pb.Increment()
	}
	pb.Finish()

	cmd.global.Verbosef("found %d of %d data blobs still in use\n", len(usedBlobs), stats.blobs)

	for packID, blobSet := range packs {
		for h := range blobSet {
			if !usedBlobs.Has(h) {
				rewritePacks.Insert(packID)
			}

			if blobCount[h.ID] > 1 {
				rewritePacks.Insert(packID)
			}
		}
	}

	cmd.global.Verbosef("will rewrite %d packs\n", len(rewritePacks))

	err = repository.Repack(repo, rewritePacks, usedBlobs)
	if err != nil {
		return err
	}

	cmd.global.Verbosef("creating new index\n")

	err = repository.RebuildIndex(repo)
	if err != nil {
		return err
	}

	cmd.global.Verbosef("done\n")
	return nil
}
