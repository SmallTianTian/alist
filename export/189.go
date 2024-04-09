//go:build 189
// +build 189

package export

import _189 "github.com/alist-org/alist/v3/drivers/189"

// usage:
// {"username": "xxx", "password": "xxx", "root_folder_id": "xxx"}
func init() {
	Storage = &_189.Cloud189{}
}
