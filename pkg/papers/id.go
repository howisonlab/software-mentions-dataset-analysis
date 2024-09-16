package papers

import (
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"strconv"
	"strings"
)

type PaperIdJson struct {
	Id string `json:"id"`

	Doi   string `json:"doi,omitempty"`
	Arxiv string `json:"arxiv,omitempty"`

	Pmid    string `json:"pmid,omitempty"`
	Pmcid   string `json:"pmcid,omitempty"`
	IstexId string `json:"istexId,omitempty"`

	Resources []string `json:"resources,omitempty"`
	License   string   `json:"license,omitempty"`

	OaLink string `json:"oa_link,omitempty"`
}

var ErrParsePaperId = errors.New("parsing PaperId")

func ToUUID(id string) (*UUID, error) {
	parsed, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("%w: parsing Id %q: %w", ErrParsePaperId, id, err)
	}

	bytes, err := parsed.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("%w: marshalling Id %q to binary: %w", ErrParsePaperId, id, err)
	}

	return &UUID{
		Id: bytes,
	}, nil
}

var ErrMarshalPaperId = errors.New("marshalling PaperId")

func UUIDToString(id *UUID) (string, error) {
	r := uuid.New()
	err := r.UnmarshalBinary(id.Id)
	if err != nil {
		return "", fmt.Errorf("%w: unmarshalling binary UUID: %w", ErrMarshalPaperId, err)
	}

	return r.String(), nil
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

func ToResourceStrings(resources []ResourceType) ([]string, error) {
	result := make([]string, len(resources))

	for i, resource := range resources {
		switch resource {
		case ResourceType_RESOURCE_JSON:
			result[i] = "json"
		case ResourceType_RESOURCE_PDF:
			result[i] = "pdf"
		case ResourceType_RESOURCE_LATEX:
			result[i] = "latex"
		case ResourceType_RESOURCE_XML:
			result[i] = "xml"
		default:
			return nil, fmt.Errorf("%w: unknown resource %q", ErrParsePaperId, resource)
		}
	}

	return result, nil
}

// ToLicenseType converts a string to the corresponding LicenseType enum.
// Does not preserve case.
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

func ToLicenseString(licenseType LicenseType) (string, error) {
	switch licenseType {
	case LicenseType_LICENSE_UNSPECIFIED:
		return "", nil
	case LicenseType_LICENSE_CC_BY:
		return "cc-by", nil
	case LicenseType_LICENSE_CC_BY_NC_ND:
		return "cc-by-nc-nd", nil
	case LicenseType_LICENSE_CC_BY_NC:
		return "cc-by-nc", nil
	case LicenseType_LICENCE_ARXIV:
		return "arXiv", nil
	case LicenseType_LICENSE_CC_BY_NC_SA:
		return "cc-by-nc-sa", nil
	case LicenseType_LICENSE_CC_BY_SA:
		return "cc-by-sa", nil
	case LicenseType_LICENSE_CC0:
		return "cc0", nil
	case LicenseType_LICENSE_ELSEVIER_SPECIFIC_OA_USER_LICENSE:
		return "elsevier-specific: oa user license", nil
	case LicenseType_LICENSE_IMPLIED_OA:
		return "implied-oa", nil
	case LicenseType_LICENSE_PUBLIC_DOMAIN:
		return "pd", nil
	case LicenseType_LICENSE_NO_CC_CODE:
		return "NO-CC CODE", nil
	case LicenseType_LICENSE_CC_BY_ND:
		return "cc-by-nd", nil
	case LicenseType_LICENSE_PUBLISHER_SPECIFIC_LICENSE:
		return "publisher-specific license", nil
	case LicenseType_LICENSE_PUBLISHER_SPECIFIC_AUTHOR_MANUSCRIPT:
		return "publisher-specific, author manuscript", nil
	case LicenseType_LICENSE_ACS_SPECIFIC_CHOICE_USAGE_AGREEMENT:
		return "acs-specific: authorchoice/editors choice usage agreement", nil
	case LicenseType_LICENSE_OPEN_GOVERNMENT_LICENSE_CANADA:
		return "Open Government Licence - Canada", nil
	default:
		return "", fmt.Errorf("%w: unknown license %q", ErrParsePaperId, licenseType)
	}
}

func ToPmid(id string) (*Pmid, error) {
	if id == "" {
		return nil, nil
	}

	// Example: 12862144

	result, err := strconv.ParseInt(id, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("%w: parsing PmId %q: %w", ErrParsePaperId, id, err)
	}

	return &Pmid{Id: uint32(result)}, nil
}

func PmidToString(id *Pmid) string {
	if id == nil {
		return ""
	}

	return fmt.Sprintf("%d", id.Id)
}

func ToPmcid(id string) (*Pmcid, error) {
	if id == "" {
		return nil, nil
	}

	// Example: PMC6665909
	id = id[3:]

	result, err := strconv.ParseInt(id, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("%w: parsing PmcId %q: %w", ErrParsePaperId, id, err)
	}

	return &Pmcid{Id: uint32(result)}, nil
}

func PmcidToString(id *Pmcid) string {
	if id == nil {
		return ""
	}

	return fmt.Sprintf("PMC%d", id.Id)
}

func ToIstexId(id string) (*IstexId, error) {
	if id == "" {
		return nil, nil
	}
	if len(id) != 40 {
		return nil, fmt.Errorf("%w: IstexId must be 40 characters long, got %d", ErrParsePaperId, len(id))
	}

	// Example: 4B98414E076FB3C1053BA36A5A2A7C2FA4ED35A1
	result := &IstexId{
		Id: make([]byte, 20),
	}

	_, err := hex.Decode(result.Id, []byte(id))
	if err != nil {
		return nil, fmt.Errorf("%w: parsing IstexId %q: %w", ErrParsePaperId, id, err)
	}

	return result, nil
}

func IstexIdToString(id *IstexId) string {
	if id == nil {
		return ""
	}

	s := hex.EncodeToString(id.Id)
	return strings.ToUpper(s)
}

var ErrParsePaperIdJson = errors.New("parsing PaperId from JSON")

func (p *PaperIdJson) MarshalProto() (*PaperId, error) {
	x := &PaperId{}
	var err error
	x.Id, err = ToUUID(p.Id)
	if err != nil {
		return nil, fmt.Errorf("%w: parsing UUID: %w", ErrParsePaperIdJson, err)
	}

	x.Doi = p.Doi
	x.Arxiv = p.Arxiv

	x.Pmid, err = ToPmid(p.Pmid)
	if err != nil {
		return nil, fmt.Errorf("%w: parsing PMID: %w", ErrParsePaperIdJson, err)
	}

	x.Pmcid, err = ToPmcid(p.Pmcid)
	if err != nil {
		return nil, fmt.Errorf("%w: parsing PMCID: %w", ErrParsePaperIdJson, err)
	}

	x.IstexId, err = ToIstexId(p.IstexId)
	if err != nil {
		return nil, fmt.Errorf("%w: parsing IstexId: %w", ErrParsePaperIdJson, err)
	}

	x.Resources, err = ToResources(p.Resources)
	if err != nil {
		return nil, fmt.Errorf("%w: parsing resources: %w", ErrParsePaperIdJson, err)
	}

	x.License, err = ToLicenseType(p.License)
	if err != nil {
		return nil, fmt.Errorf("%w: parsing license: %w", ErrParsePaperIdJson, err)
	}

	x.OaLink = p.OaLink

	return x, nil
}

func (p *PaperIdJson) UnmarshalProto(x *PaperId) error {
	var err error
	p.Id, err = UUIDToString(x.Id)
	if err != nil {
		return err
	}

	p.Doi = x.Doi
	p.Arxiv = x.Arxiv

	p.Pmid = PmidToString(x.Pmid)
	p.Pmcid = PmcidToString(x.Pmcid)
	p.IstexId = IstexIdToString(x.IstexId)

	p.Resources, err = ToResourceStrings(x.Resources)
	if err != nil {
		return err
	}

	p.License, err = ToLicenseString(x.License)
	if err != nil {
		return err
	}

	p.OaLink = x.OaLink

	return nil
}
