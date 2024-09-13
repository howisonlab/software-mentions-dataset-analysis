package papers

import (
	"errors"
	"fmt"
	"strconv"
)

type PaperIdJson struct {
	PaperId

	IdString         string   `json:"id"`
	ResourcesStrings []string `json:"resources"`
	LicenseString    string   `json:"license"`

	PmIdString    string `json:"pmid"`
	PmcIdString   string `json:"pmcid"`
	IstexIdString string `json:"istexId"`
}

var ErrParsePaperId = errors.New("parsing PaperId")

func ToId(id string) (uint64, uint64, error) {
	// Example: fff5b2e5-a467-4ea1-a78a-4d3027729b5d
	s01 := id[0:8]
	s2 := id[9:13]
	s3 := id[14:18]
	s4 := id[19:23]
	s567 := id[24:]
	sA := fmt.Sprintf("%s%s%s", s01, s2, s3)
	sB := fmt.Sprintf("%s%s", s4, s567)

	result1, err := strconv.ParseUint(sA, 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: parsing Id %q: %w", ErrParsePaperId, id, err)
	}

	result2, err := strconv.ParseUint(sB, 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: parsing Id %q: %w", ErrParsePaperId, id, err)
	}

	return result1, result2, nil
}

func ToResources(resources []string) ([]ResourceType, error) {
	result := make([]ResourceType, len(resources))

	for i, resource := range resources {
		switch resource {
		case "json":
			result[i] = ResourceType_RESOURCE_JSON
		case "pdf":
			result[i] = ResourceType_RESOURCE_PDF
		case "latex":
			result[i] = ResourceType_RESOURCE_LATEX
		case "xml":
			result[i] = ResourceType_RESOURCE_XML
		default:
			return nil, fmt.Errorf("%w: unknown resource %q", ErrParsePaperId, resource)
		}
	}

	return result, nil
}

func ToLicenseType(licenseType string) (LicenseType, error) {
	switch licenseType {
	case "":
		return LicenseType_LICENSE_UNSPECIFIED, nil
	case "cc-by", "CC BY":
		return LicenseType_LICENSE_CC_BY, nil
	case "cc-by-nc-nd", "CC BY-NC-ND":
		return LicenseType_LICENSE_CC_BY_NC_ND, nil
	case "cc-by-nc", "CC BY-NC":
		return LicenseType_LICENSE_CC_BY_NC, nil
	case "arXiv":
		return LicenseType_LICENCE_ARXIV, nil
	case "cc-by-nc-sa", "CC BY-NC-SA":
		return LicenseType_LICENSE_CC_BY_NC_SA, nil
	case "cc-by-sa", "CC BY-SA":
		return LicenseType_LICENSE_CC_BY_SA, nil
	case "cc0", "CC0":
		return LicenseType_LICENSE_CC0, nil
	case "elsevier-specific: oa user license":
		return LicenseType_LICENSE_ELSEVIER_SPECIFIC_OA_USER_LICENSE, nil
	case "implied-oa":
		return LicenseType_LICENSE_IMPLIED_OA, nil
	case "pd":
		return LicenseType_LICENSE_PUBLIC_DOMAIN, nil
	case "NO-CC CODE":
		return LicenseType_LICENSE_NO_CC_CODE, nil
	case "cc-by-nd", "CC BY-ND":
		return LicenseType_LICENSE_CC_BY_ND, nil
	case "publisher-specific license":
		return LicenseType_LICENSE_PUBLISHER_SPECIFIC_LICENSE, nil
	case "publisher-specific, author manuscript":
		return LicenseType_LICENSE_PUBLISHER_SPECIFIC_AUTHOR_MANUSCRIPT, nil
	case "acs-specific: authorchoice/editors choice usage agreement":
		return LicenseType_LICENSE_ACS_SPECIFIC_CHOICE_USAGE_AGREEMENT, nil
	case "Open Government Licence - Canada":
		return LicenseType_LICENSE_OPEN_GOVERNMENT_LICENSE_CANADA, nil
	default:
		return LicenseType_LICENSE_UNSPECIFIED, fmt.Errorf("%w: unknown license %q", ErrParsePaperId, licenseType)
	}
}

func ToPmId(id string) (uint32, error) {
	if id == "" {
		return 0, nil
	}

	// Example: 12862144

	result, err := strconv.ParseInt(id, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%w: parsing PmId %q: %w", ErrParsePaperId, id, err)
	}

	return uint32(result), nil
}

func ToPmcId(id string) (uint32, error) {
	if id == "" {
		return 0, nil
	}

	// Example: PMC6665909
	id = id[3:]

	result, err := strconv.ParseInt(id, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%w: parsing PmcId %q: %w", ErrParsePaperId, id, err)
	}

	return uint32(result), nil
}

func ToIstexId(id string) (uint64, uint64, uint32, error) {
	if id == "" {
		return 0, 0, 0, nil
	}

	// Example: 4B98414E076FB3C1053BA36A5A2A7C2FA4ED35A1
	s0 := id[0:16]
	s1 := id[16:32]
	s2 := id[32:]

	result0, err := strconv.ParseUint(s0, 16, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%w: parsing IstexId %q(%q): %w", ErrParsePaperId, id, s0, err)
	}
	result1, err := strconv.ParseUint(s1, 16, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%w: parsing IstexId %q(%q): %w", ErrParsePaperId, id, s1, err)
	}
	result2, err := strconv.ParseUint(s2, 16, 32)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("%w: parsing IstexId %q(%q): %w", ErrParsePaperId, id, s2, err)
	}

	return result0, result1, uint32(result2), nil
}
