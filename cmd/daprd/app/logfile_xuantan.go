/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package app

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// resolveXuantanLogFile 把 --log-file 给的「逻辑文件名」改写成带时间戳的实际文件名，并预建目录：
//
//	/xt/logs/dapr/biz/xt-user.log → /xt/logs/dapr/biz/xt-user_2026_08_01_204540804.log
//
// 两个原因：
//   - kit 的 setLogOutput 只做 os.OpenFile，父目录不存在即 Fatal；
//   - 各 daprd 共享同一日志 PVC，固定文件名会让滚动更新期间新旧 Pod 追加进同一文件。
//
// 时间戳格式对齐业务进程的 core/xlog（app.log → app_2026_07_24_010203456.log），便于统一检索。
// path 为空表示不落文件（kit 回退 stdout），原样返回。
func resolveXuantanLogFile(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create log dir %q: %w", dir, err)
	}
	return filepath.Join(dir, stampedXuantanLogName(filepath.Base(path), time.Now())), nil
}

// stampedXuantanLogName 对齐 core/xlog internal.stampedName：app.log → app_2026_07_24_010203456.log。
func stampedXuantanLogName(filename string, t time.Time) string {
	ext := filepath.Ext(filename)
	base := strings.TrimSuffix(filename, ext)
	stamp := t.Format("2006_01_02_150405") + fmt.Sprintf("%03d", t.Nanosecond()/1e6)
	return fmt.Sprintf("%s_%s%s", base, stamp, ext)
}
