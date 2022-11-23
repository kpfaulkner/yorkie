//go:build amd64

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sqlite_test

import (
	"context"
	"fmt"
	"log"
	"testing"
	gotime "time"

	"github.com/stretchr/testify/assert"
	monkey "github.com/undefinedlabs/go-mpatch"

	"github.com/yorkie-team/yorkie/server/backend/database"
	"github.com/yorkie-team/yorkie/server/backend/database/memory"
)

func TestHousekeeping(t *testing.T) {
	memdb, err := memory.New()
	assert.NoError(t, err)
	projectID := database.DefaultProjectID

	t.Run("housekeeping test", func(t *testing.T) {
		ctx := context.Background()

		yesterday := gotime.Now().Add(-24 * gotime.Hour)
		patch, err := monkey.PatchMethod(gotime.Now, func() gotime.Time { return yesterday })
		if err != nil {
			log.Fatal(err)
		}
		clientA, err := memdb.ActivateClient(ctx, projectID, fmt.Sprintf("%s-A", t.Name()))
		assert.NoError(t, err)
		clientB, err := memdb.ActivateClient(ctx, projectID, fmt.Sprintf("%s-B", t.Name()))
		assert.NoError(t, err)
		err = patch.Unpatch()
		if err != nil {
			log.Fatal(err)
		}

		clientC, err := memdb.ActivateClient(ctx, projectID, fmt.Sprintf("%s-C", t.Name()))
		assert.NoError(t, err)

		candidates, err := memdb.FindDeactivateCandidates(
			ctx,
			gotime.Hour,
			10,
		)
		assert.NoError(t, err)
		assert.Len(t, candidates, 2)
		assert.Contains(t, candidates, clientA)
		assert.Contains(t, candidates, clientB)
		assert.NotContains(t, candidates, clientC)
	})
}
