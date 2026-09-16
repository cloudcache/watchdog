package watchdog

import "context"

// fakeSNMPRepository supports the remaining legacy network/discovery tests.
// The retired SNMP profile/MIB HTTP tests no longer own this shared fake.
type fakeSNMPRepository struct {
	profiles []SNMPProfile
	modules  []MIBModule
}

func (r *fakeSNMPRepository) ListSNMPProfiles(context.Context, ID) ([]SNMPProfile, error) {
	return r.profiles, nil
}

func (r *fakeSNMPRepository) GetSNMPProfile(_ context.Context, _ ID, profileID ID) (SNMPProfile, error) {
	for _, profile := range r.profiles {
		if profile.ID == profileID {
			return profile, nil
		}
	}
	return SNMPProfile{}, errNotFoundForTest{}
}

func (r *fakeSNMPRepository) UpsertSNMPProfile(_ context.Context, profile SNMPProfile) (SNMPProfile, error) {
	r.profiles = append(r.profiles, profile)
	return profile, nil
}

func (r *fakeSNMPRepository) DeleteSNMPProfile(_ context.Context, _ ID, profileID ID) error {
	for index, profile := range r.profiles {
		if profile.ID == profileID {
			r.profiles = append(r.profiles[:index], r.profiles[index+1:]...)
			return nil
		}
	}
	return nil
}

func (r *fakeSNMPRepository) ListMIBModules(context.Context) ([]MIBModule, error) {
	return r.modules, nil
}

func (r *fakeSNMPRepository) UpsertMIBModule(_ context.Context, module MIBModule) (MIBModule, error) {
	if module.ID == "" {
		module.ID = string(stableID("mib", module.Source, module.Name))
	}
	r.modules = append(r.modules, module)
	return module, nil
}

func (r *fakeSNMPRepository) DeleteMIBModule(context.Context, ID) error {
	r.modules = nil
	return nil
}
