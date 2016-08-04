package main

import (
	"fmt"
	"restic"
	"restic/backend"
	"restic/debug"
	"restic/pack"
	"restic/repository"
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

	for packID := range packs {
		debug.Log("CmdPrune.Execute", "process pack %v", packID.Str())
		cmd.global.Verbosef("process pack %v\n", packID.Str())

		list, err := repo.ListPack(packID)
		if err != nil {
			cmd.global.Warnf("unable to list pack %v: %v\n", packID.Str(), err)
			continue
		}

		debug.Log("CmdPrune.Execute", "pack %v contains %d blobs", packID.Str(), len(list))
		for _, pb := range list {
			cmd.global.Verbosef("  blob %v (%v)\n", pb.ID.Str(), pb.Type)
			packs[packID].Insert(pack.Handle{ID: pb.ID, Type: pb.Type})
			stats.blobs++
			blobCount[pb.ID]++
		}
	}

	for id, num := range blobCount {
		if num <= 1 {
			continue
		}

		cmd.global.Verbosef("blob %v stored %d times:\n", id.Str(), num)
		for packID, packBlobs := range packs {
			for h := range packBlobs {
				if h.ID.Equal(id) {
					fmt.Printf("   pack %v, %v\n", packID.Str(), h)
				}
			}
		}
	}

	cmd.global.Verbosef("load all snapshots\n")

	snapshots, err := restic.LoadAllSnapshots(repo)
	if err != nil {
		return err
	}

	stats.snapshots = len(snapshots)

	cmd.global.Verbosef("find data that is still in use for %d snapshots\n", stats.snapshots)

	usedBlobs := pack.NewBlobSet()
	for _, sn := range snapshots {
		debug.Log("CmdPrune.Execute", "process snapshot %v", sn.ID().Str())

		err = restic.FindUsedBlobs(repo, *sn.Tree, usedBlobs)
		if err != nil {
			return err
		}

		debug.Log("CmdPrune.Execute", "found %v blobs for snapshot %v", sn.ID().Str())
	}

	cmd.global.Verbosef("found %d of %d data blobs still in use\n", len(usedBlobs), stats.blobs)

	rewritePacks := backend.NewIDSet()
	for packID, blobSet := range packs {
		for h := range blobSet {
			if !usedBlobs.Has(h) {
				cmd.global.Verbosef("blob %v is unused\n", h)
				rewritePacks.Insert(packID)
			}
		}
	}

	err = repository.Repack(repo, rewritePacks, usedBlobs)
	if err != nil {
		return err
	}

	return repository.RebuildIndex(repo)
}
