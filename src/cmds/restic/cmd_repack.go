// +build debug

package main

import (
	"restic/backend"
	"restic/debug"
	"restic/repository"
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

	done := make(chan struct{})
	defer close(done)

	duplicateBlobs := make(map[backend.ID]uint)

	packs := backend.NewIDSet()
	blobs := backend.NewIDSet()

	cmd.global.Verbosef("listing packs in repo\n")
	for packID := range repo.List(backend.Data, done) {
		debug.Log("CmdRepack.Execute", "process pack %v", packID.Str())

		list, err := repo.ListPack(packID)
		if err != nil {
			cmd.global.Warnf("unable to list pack %v: %v\n", packID.Str(), err)
			continue
		}

		debug.Log("CmdRepack.Execute", "pack %v contains %d blobs", packID.Str(), len(list))
		for _, pb := range list {
			blobs.Insert(pb.ID)
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
