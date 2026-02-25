package meta

// Migrations handles schema versioning for the BoltDB metadata store.
// Currently at version 1 - no migrations needed yet.
// Future migrations will be added here as the schema evolves.

// Migrate runs any pending schema migrations.
func (s *BoltStore) Migrate() error {
	// Version 1 is the initial schema, no migrations needed.
	return nil
}
