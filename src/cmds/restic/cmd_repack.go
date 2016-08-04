// +build debug

package main

import (
	"restic/backend"
	"restic/debug"
	"restic/pack"
	"restic/repository"
	"restic/worker"
)

// CmdRepack implements the 'repack' command.
type CmdRepack struct {
	global *GlobalOptions
}

func init() {
	_, err := parser.AddCommand("repack",
		"repacks a repository",
		"The repack command removes rendundant data from the repository",
		&CmdRepack{global: &globalOpts})
	if err != nil {
		panic(err)
	}
}

// Execute runs the 'repack' command.
func (cmd CmdRepack) Execute(args []string) error {
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

	duplicateBlobs := make(map[backend.ID]uint)

	packs := backend.NewIDSet()
	blobs := pack.NewBlobSet()

	cmd.global.Verbosef("listing packs in repo\n")

	done := make(chan struct{})
	defer close(done)
	ch := make(chan worker.Job)
	go repository.ListAllPacks(repo, ch, done)

	for job := range ch {
		packID := job.Data.(backend.ID)
		if job.Error != nil {
			cmd.global.Warnf("unable to list pack %v: %v\n", packID.Str(), job.Error)
			continue
		}

		j := job.Result.(repository.ListAllPacksResult)

		debug.Log("CmdRepack.Execute", "pack %v contains %d blobs", j.PackID.Str(), len(j.Entries))
		for _, pb := range j.Entries {
			blobs.Insert(pack.Handle{ID: pb.ID, Type: pb.Type})
			duplicateBlobs[pb.ID]++

			// only insert it into the list of packs when there is a blob we
			// have already seen.
			if duplicateBlobs[pb.ID] > 1 {
				packs.Insert(packID)
			}
		}
	}

	cmd.global.Printf("%v unique blobs, %v packs\n", len(blobs), len(packs))

	dups := 0
	for _, v := range duplicateBlobs {
		if v <= 1 {
			continue
		}

		dups++
	}
	cmd.global.Printf("  %d duplicate blobs\n", dups)

	if err := repository.Repack(repo, packs, blobs); err != nil {
		return err
	}

	return repository.RebuildIndex(repo)
}
