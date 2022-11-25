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

// Package sqlite implements the database interface using sqlite database.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	gotime "time"

	"github.com/hashicorp/go-memdb"
	"go.mongodb.org/mongo-driver/bson/primitive"
	_ "modernc.org/sqlite"

	"github.com/yorkie-team/yorkie/api/converter"
	"github.com/yorkie-team/yorkie/api/types"
	"github.com/yorkie-team/yorkie/pkg/document"
	"github.com/yorkie-team/yorkie/pkg/document/change"
	"github.com/yorkie-team/yorkie/pkg/document/key"
	"github.com/yorkie-team/yorkie/pkg/document/time"
	"github.com/yorkie-team/yorkie/server/backend/database"
)

// DB is an in-memory database for testing or temporarily.
type DB struct {
	conn *sql.Conn
}

// New returns a new in-memory database.
func New(filename string) (*DB, error) {

	db, err := sql.Open("sqlite", filename)
	if err != nil {
		return nil, fmt.Errorf("new sqlitedb: %w", err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, fmt.Errorf("new sqlitedb: %w", err)
	}

	return &DB{
		conn: conn,
	}, nil
}

// Close closes the database.
func (d *DB) Close() error {
	d.conn.Close()
	return nil
}

func XXXreadRowIntoProjectInfo(row *sql.Row) (*database.ProjectInfo, error) {
	var id types.ID
	var nameDB string
	var ownerDB types.ID
	var publicKeyDB string
	var secretKey string
	var authWebhoolURL string
	var authWebhookMethods []string
	var createdAt gotime.Time
	var updatedAt gotime.Time

	err := row.Scan(&id, &nameDB, &ownerDB, &publicKeyDB, &secretKey, &authWebhoolURL, &authWebhookMethods, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}

	projectInfo := database.ProjectInfo{
		ID:                 id,
		Name:               nameDB,
		Owner:              ownerDB,
		PublicKey:          publicKeyDB,
		SecretKey:          secretKey,
		AuthWebhookURL:     authWebhoolURL,
		AuthWebhookMethods: authWebhookMethods,
		CreatedAt:          createdAt,
		UpdatedAt:          updatedAt,
	}

	return &projectInfo, nil
}

func readRowIntoProjectInfo(scan func(dest ...any) error) (*database.ProjectInfo, error) {
	var id types.ID
	var nameDB string
	var ownerDB types.ID
	var publicKeyDB string
	var secretKey string
	var authWebhoolURL string
	var authWebhookMethods []string
	var createdAt gotime.Time
	var updatedAt gotime.Time

	err := scan(&id, &nameDB, &ownerDB, &publicKeyDB, &secretKey, &authWebhoolURL, &authWebhookMethods, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}

	projectInfo := database.ProjectInfo{
		ID:                 id,
		Name:               nameDB,
		Owner:              ownerDB,
		PublicKey:          publicKeyDB,
		SecretKey:          secretKey,
		AuthWebhookURL:     authWebhoolURL,
		AuthWebhookMethods: authWebhookMethods,
		CreatedAt:          createdAt,
		UpdatedAt:          updatedAt,
	}

	return &projectInfo, nil
}

// TODO(kpfaulkner) fix documents map!!!
func readRowIntoClientInfo(scan func(dest ...any) error) (*database.ClientInfo, error) {

	var id types.ID
	var projectID types.ID
	var key string
	var status string
	var documents map[types.ID]*database.ClientDocInfo
	var createdAt gotime.Time
	var updatedAt gotime.Time

	// documents expected to initially blow up. TODO(kpfaulkner)
	err := scan(&id, &projectID, &key, &status, &documents, &createdAt, &updatedAt)
	if err != nil {
		return nil, err
	}

	clientInfo := database.ClientInfo{
		ID:        id,
		ProjectID: projectID,
		Key:       key,
		Status:    status,
		Documents: documents,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}

	return &clientInfo, nil
}

func readRowIntoDocInfo(scan func(dest ...any) error) (*database.DocInfo, error) {

	var id types.ID
	var projectID types.ID
	var key key.Key
	var serverSeq int64
	var owner types.ID
	var createdAt gotime.Time
	var accessedAt gotime.Time
	var updatedAt gotime.Time

	err := scan(&id, &projectID, &key, &serverSeq, &owner, &createdAt, &accessedAt, &updatedAt)
	if err != nil {
		return nil, err
	}

	docInfo := database.DocInfo{
		ID:         id,
		ProjectID:  projectID,
		Key:        key,
		ServerSeq:  serverSeq,
		Owner:      owner,
		CreatedAt:  createdAt,
		AccessedAt: accessedAt,
		UpdatedAt:  updatedAt,
	}

	return &docInfo, nil
}

func readRowIntoUserInfo(scan func(dest ...any) error) (*database.UserInfo, error) {
	var id types.ID
	var username string
	var hashedPassword string
	var createdAt gotime.Time

	err := scan(&id, &username, &hashedPassword, &createdAt)
	if err != nil {
		return nil, err
	}

	userInfo := database.UserInfo{
		ID:             id,
		Username:       username,
		HashedPassword: hashedPassword,
		CreatedAt:      createdAt,
	}

	return &userInfo, nil
}

// FindProjectInfoByPublicKey returns a project by public key.
func (d *DB) FindProjectInfoByPublicKey(
	ctx context.Context,
	publicKey string,
) (*database.ProjectInfo, error) {
	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("unable to create transaction: %w", err)
	}

	defer txn.Rollback()
	rows := txn.QueryRowContext(ctx, "SELECT * FROM ? WHERE public_key = ?", tblProjects, publicKey)
	projectInfo, err := readRowIntoProjectInfo(rows.Scan)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", publicKey, database.ErrProjectNotFound)
	}
	return projectInfo.DeepCopy(), nil
}

// FindProjectInfoByName returns a project by the given name.
func (d *DB) FindProjectInfoByName(
	ctx context.Context,
	owner types.ID,
	name string,
) (*database.ProjectInfo, error) {
	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("unable to create transaction: %w", err)
	}

	// unsure about rollback here. Basically an abort... but if devops
	// are monitoring the database, they might see a lot of aborted
	// TODO(kpfaulkner)
	defer txn.Rollback()

	rows := txn.QueryRowContext(ctx, "SELECT * FROM ? WHERE owner = ? AND name = ?", tblProjects, owner.String(), name)
	projectInfo, err := readRowIntoProjectInfo(rows.Scan)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%s: %w", name, database.ErrProjectNotFound)
		}
		return nil, fmt.Errorf("find project by owner and name: %w", err)
	}

	return projectInfo.DeepCopy(), nil
}

// FindProjectInfoByID returns a project by the given id.
func (d *DB) FindProjectInfoByID(ctx context.Context, id types.ID) (*database.ProjectInfo, error) {
	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("unable to create transaction: %w", err)
	}

	// unsure about rollback here. Basically an abort... but if devops
	// are monitoring the database, they might see a lot of aborted
	// TODO(kpfaulkner)
	defer txn.Rollback()

	rows := txn.QueryRowContext(ctx, "SELECT * FROM ? WHERE id = ?", tblProjects, id)

	projectInfo, err := readRowIntoProjectInfo(rows.Scan)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%s: %w", id, database.ErrProjectNotFound)
		}
		return nil, fmt.Errorf("find project by id: %w", err)
	}

	return projectInfo.DeepCopy(), nil
}

// EnsureDefaultUserAndProject creates the default user and project if they do not exist.
func (d *DB) EnsureDefaultUserAndProject(
	ctx context.Context,
	username,
	password string,
) (*database.UserInfo, *database.ProjectInfo, error) {
	user, err := d.ensureDefaultUserInfo(ctx, username, password)
	if err != nil {
		return nil, nil, err
	}

	project, err := d.ensureDefaultProjectInfo(ctx, user.ID)
	if err != nil {
		return nil, nil, err
	}

	return user, project, nil
}

// ensureDefaultUserInfo creates the default user if it does not exist.
func (d *DB) ensureDefaultUserInfo(
	ctx context.Context,
	username,
	password string,
) (*database.UserInfo, error) {
	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, fmt.Errorf("unable to create transaction: %w", err)
	}

	defer txn.Rollback()

	rows := txn.QueryRowContext(ctx, "SELECT * FROM ? WHERE owner = ? AND name = ?", tblUsers, username)
	info, err := readRowIntoUserInfo(rows.Scan)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("find user by username: %w", err)
	}

	if err == sql.ErrNoRows {
		hashedPassword, err := database.HashedPassword(password)
		if err != nil {
			return nil, err
		}
		info = database.NewUserInfo(username, hashedPassword)
		info.ID = newID()

		if _, err := txn.ExecContext(ctx, "INSERT INTO ? VALUES(?, ?, ?, ?)", tblUsers, info.ID, info.Username, info.HashedPassword, info.CreatedAt); err != nil {
			return nil, fmt.Errorf("insert user: %w", err)
		}
	}

	txn.Commit()
	return info, nil
}

// ensureDefaultProjectInfo creates the default project if it does not exist.
func (d *DB) ensureDefaultProjectInfo(
	ctx context.Context,
	defaultUserID types.ID,
) (*database.ProjectInfo, error) {
	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, fmt.Errorf("unable to create transaction: %w", err)
	}

	defer txn.Rollback()
	rows := txn.QueryRowContext(ctx, "SELECT * FROM ? WHERE id = ?", tblProjects, defaultUserID)

	info, err := readRowIntoProjectInfo(rows.Scan)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("find default project: %w", err)
	}

	if err == sql.ErrNoRows {
		info = database.NewProjectInfo(database.DefaultProjectName, defaultUserID)
		info.ID = database.DefaultProjectID
		if err = insertProjectInfo(ctx, txn, info); err != nil {
			return nil, fmt.Errorf("insert project: %w", err)
		}
	}
	txn.Commit()
	return info, nil
}

func insertProjectInfo(ctx context.Context, txn *sql.Tx, info *database.ProjectInfo) error {
	_, err := txn.ExecContext(ctx, "INSERT INTO ? VALUES(?,?,?,?,?,?,?,?,?);", tblProjects, info.ID, info.Name,
		info.Owner, info.PublicKey, info.SecretKey, info.AuthWebhookURL, info.AuthWebhookMethods, info.CreatedAt, info.UpdatedAt)
	return err
}

func insertUserInfo(ctx context.Context, txn *sql.Tx, info *database.UserInfo) error {
	_, err := txn.ExecContext(ctx, "INSERT INTO ? VALUES(?,?,?,?);", tblUsers, info.ID, info.Username, info.HashedPassword, info.CreatedAt)
	return err
}

// CreateProjectInfo creates a new project.
func (d *DB) CreateProjectInfo(
	ctx context.Context,
	name string,
	owner types.ID,
) (*database.ProjectInfo, error) {
	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, fmt.Errorf("unable to create transaction: %w", err)
	}
	defer txn.Rollback()

	rows := txn.QueryRowContext(ctx, "SELECT * FROM ? WHERE owner = ? AND name = ?", tblProjects, owner.String(), name)
	projectInfo, err := readRowIntoProjectInfo(rows.Scan)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("find project by owner and name: %w", err)
	}

	if projectInfo != nil {
		return nil, fmt.Errorf("%s: %w", name, database.ErrProjectAlreadyExists)
	}

	info := database.NewProjectInfo(name, owner)
	info.ID = newID()

	if err := insertProjectInfo(ctx, txn, info); err != nil {
		return nil, fmt.Errorf("insert user: %w", err)
	}
	txn.Commit()

	return info, nil
}

// ListProjectInfos returns all projects.
func (d *DB) ListProjectInfos(
	ctx context.Context,
	owner types.ID,
) ([]*database.ProjectInfo, error) {
	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("unable to create transaction: %w", err)
	}
	defer txn.Rollback()

	var infos []*database.ProjectInfo
	rows, err := txn.QueryContext(ctx, "SELECT * FROM ? WHERE owner_name = ?", tblProjects, owner.String())
	for rows.Next() {
		pi, err := readRowIntoProjectInfo(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("read row into project info: %w", err)
		}
		info := pi.DeepCopy()
		if info.Owner != owner {
			break
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// UpdateProjectInfo updates the given project.
func (d *DB) UpdateProjectInfo(
	ctx context.Context,
	owner types.ID,
	id types.ID,
	fields *types.UpdatableProjectFields,
) (*database.ProjectInfo, error) {
	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, fmt.Errorf("unable to create transaction: %w", err)
	}

	defer txn.Rollback()

	rows := txn.QueryRowContext(ctx, "SELECT * FROM ? WHERE id = ?", tblProjects, id)
	projectInfo, err := readRowIntoProjectInfo(rows.Scan)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%s: %w", id, database.ErrProjectNotFound)
		}
		return nil, fmt.Errorf("find project by id: %w", err)
	}

	info := projectInfo.DeepCopy()
	if info.Owner != owner {
		return nil, fmt.Errorf("%s: %w", id, database.ErrProjectNotFound)
	}

	if fields.Name != nil {
		existing, err := d.FindProjectInfoByName(ctx, owner, *fields.Name)
		if err != nil {
			return nil, fmt.Errorf("find project by owner and name: %w", err)
		}
		if existing != nil && info.Name != *fields.Name {
			return nil, fmt.Errorf("%s: %w", *fields.Name, database.ErrProjectNameAlreadyExists)
		}
	}

	info.UpdateFields(fields)
	info.UpdatedAt = gotime.Now()

	// update... we KNOW it already exists (checks above).
	if _, err := txn.ExecContext(ctx, "UPDATE ? SET name=?,authwebhook=?, authwebhookmethods=?, updatedat=? where id=?;", tblProjects, info.Name,
		info.AuthWebhookURL, info.AuthWebhookMethods, info.UpdatedAt, info.ID); err != nil {
		return nil, fmt.Errorf("update project: %w", err)
	}
	txn.Commit()
	return info, nil
}

// CreateUserInfo creates a new user.
func (d *DB) CreateUserInfo(
	ctx context.Context,
	username string,
	hashedPassword string,
) (*database.UserInfo, error) {
	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, fmt.Errorf("unable to create transaction: %w", err)
	}
	defer txn.Rollback()

	row := txn.QueryRowContext(ctx, "SELECT * FROM ? WHERE username = ?", tblUsers, username)
	userInfo, err := readRowIntoUserInfo(row.Scan)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("find username: %w", err)
	}

	if userInfo != nil {
		return nil, fmt.Errorf("%s: %w", username, database.ErrUserAlreadyExists)
	}

	info := database.NewUserInfo(username, hashedPassword)
	info.ID = newID()

	if err := insertUserInfo(ctx, txn, info); err != nil {
		return nil, fmt.Errorf("insert user: %w", err)
	}
	txn.Commit()

	return info, nil
}

// FindUserInfo finds a user by the given username.
func (d *DB) FindUserInfo(ctx context.Context, username string) (*database.UserInfo, error) {
	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("unable to create transaction: %w", err)
	}

	// unsure about rollback here. Basically an abort... but if devops
	// are monitoring the database, they might see a lot of aborted
	// TODO(kpfaulkner)
	defer txn.Rollback()

	row := txn.QueryRowContext(ctx, "SELECT * FROM ? WHERE username = ? ", tblUsers, username)
	userInfo, err := readRowIntoUserInfo(row.Scan)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%s: %w", username, database.ErrUserNotFound)
		}
		return nil, fmt.Errorf("find user by username: %w", err)
	}

	return userInfo.DeepCopy(), nil
}

// ListUserInfos returns all users.
func (d *DB) ListUserInfos(
	ctx context.Context,
) ([]*database.UserInfo, error) {
	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("unable to create transaction: %w", err)
	}
	defer txn.Rollback()

	var infos []*database.UserInfo
	rows, err := txn.QueryContext(ctx, "SELECT * FROM ?", tblUsers)
	if err != nil {
		return nil, fmt.Errorf("fetch users: %w", err)
	}

	for rows.Next() {
		pi, err := readRowIntoUserInfo(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("read row into user info: %w", err)
		}
		info := pi.DeepCopy()
		infos = append(infos, info)
	}
	return infos, nil
}

func updateClientInfo(ctx context.Context, txn *sql.Tx, info *database.ClientInfo) error {
	// unsure what to do about ClientInfo.Documents. Instead of making separate table and joining, I'm (just for now) going to marshal to JSON and store in DB.
	bytes, err := json.Marshal(info.Documents)
	if err != nil {
		return fmt.Errorf("unable to marshal ClientInfo.Documents: %w", err)
	}

	_, err = txn.ExecContext(ctx, "UPDATE ? SET projectid=?, key=?, status=?, documents=?, createdat=?,updatedat=? WHERE id=?",
		tblClients, info.ProjectID, info.Key, info.Status, string(bytes), info.CreatedAt, info.UpdatedAt, info.ID)
	return err
}

// ActivateClient activates a client.
func (d *DB) ActivateClient(
	ctx context.Context,
	projectID types.ID,
	key string,
) (*database.ClientInfo, error) {

	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, fmt.Errorf("unable to create transaction: %w", err)
	}

	defer txn.Rollback()

	rows := txn.QueryRowContext(ctx, "SELECT * FROM ? WHERE projectid = ? AND key = ?", tblClients, projectID, key)
	existingClientInfo, err := readRowIntoClientInfo(rows.Scan)

	noClientExists := false
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("find client by project id and key: %w", err)
	} else if err == sql.ErrNoRows {
		noClientExists = true
	}

	now := gotime.Now()

	clientInfo := &database.ClientInfo{
		ProjectID: projectID,
		Key:       key,
		Status:    database.ClientActivated,
		UpdatedAt: now,
	}

	if noClientExists {
		clientInfo.ID = newID()
		clientInfo.CreatedAt = now
	} else {
		clientInfo.ID = existingClientInfo.ID
		clientInfo.CreatedAt = existingClientInfo.CreatedAt
	}

	if _, err := txn.ExecContext(ctx, "INSERT INTO ? ('id', 'projectId', 'key', 'status', 'createdat') VALUES(?, ?, ?, ?,?)", tblClients,
		clientInfo.ID, clientInfo.ProjectID, clientInfo.Key, clientInfo.Status, clientInfo.CreatedAt); err != nil {
		return nil, fmt.Errorf("insert client: %w", err)
	}
	txn.Commit()
	return clientInfo, nil
}

// DeactivateClient deactivates a client.
func (d *DB) DeactivateClient(ctx context.Context, projectID, clientID types.ID) (*database.ClientInfo, error) {
	if err := clientID.Validate(); err != nil {
		return nil, err
	}

	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, fmt.Errorf("unable to create transaction: %w", err)
	}

	defer txn.Rollback()

	rows := txn.QueryRowContext(ctx, "SELECT * FROM ? WHERE id = ? ", tblClients, clientID)
	clientInfo, err := readRowIntoClientInfo(rows.Scan)

	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("find client by id: %w", err)
	} else if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%s: %w", clientID, database.ErrClientNotFound)
	}

	if err := clientInfo.CheckIfInProject(projectID); err != nil {
		return nil, err
	}

	clientInfo = clientInfo.DeepCopy()
	clientInfo.Deactivate()
	if err := updateClientInfo(ctx, txn, clientInfo); err != nil {
		return nil, fmt.Errorf("update client: %w", err)
	}

	txn.Commit()
	return clientInfo, nil
}

// FindClientInfoByID finds a client by ID.
func (d *DB) FindClientInfoByID(ctx context.Context, projectID, clientID types.ID) (*database.ClientInfo, error) {
	if err := clientID.Validate(); err != nil {
		return nil, err
	}

	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, fmt.Errorf("unable to create transaction: %w", err)
	}

	defer txn.Rollback()

	rows := txn.QueryRowContext(ctx, "SELECT * FROM ? WHERE id = ? ", tblClients, clientID)
	clientInfo, err := readRowIntoClientInfo(rows.Scan)

	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("find client by id: %w", err)
	} else if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%s: %w", clientID, database.ErrClientNotFound)
	}

	if err := clientInfo.CheckIfInProject(projectID); err != nil {
		return nil, err
	}

	return clientInfo.DeepCopy(), nil
}

// UpdateClientInfoAfterPushPull updates the client from the given clientInfo
// after handling PushPull.
func (d *DB) UpdateClientInfoAfterPushPull(
	ctx context.Context,
	clientInfo *database.ClientInfo,
	docInfo *database.DocInfo,
) error {
	clientDocInfo := clientInfo.Documents[docInfo.ID]
	attached, err := clientInfo.IsAttached(docInfo.ID)
	if err != nil {
		return err
	}

	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return fmt.Errorf("unable to create transaction: %w", err)
	}

	defer txn.Rollback()

	rows := txn.QueryRowContext(ctx, "SELECT * FROM ? WHERE id = ? ", tblClients, clientInfo.ID)
	loaded, err := readRowIntoClientInfo(rows.Scan)

	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("find client by id: %w", err)
	} else if err == sql.ErrNoRows {
		return fmt.Errorf("%s: %w", clientInfo.ID, database.ErrClientNotFound)
	}

	if !attached {
		loaded.Documents[docInfo.ID] = &database.ClientDocInfo{
			Status: clientDocInfo.Status,
		}
		loaded.UpdatedAt = gotime.Now()
	} else {
		if _, ok := loaded.Documents[docInfo.ID]; !ok {
			loaded.Documents[docInfo.ID] = &database.ClientDocInfo{}
		}

		loadedClientDocInfo := loaded.Documents[docInfo.ID]
		serverSeq := loadedClientDocInfo.ServerSeq
		if clientDocInfo.ServerSeq > loadedClientDocInfo.ServerSeq {
			serverSeq = clientDocInfo.ServerSeq
		}
		clientSeq := loadedClientDocInfo.ClientSeq
		if clientDocInfo.ClientSeq > loadedClientDocInfo.ClientSeq {
			clientSeq = clientDocInfo.ClientSeq
		}
		loaded.Documents[docInfo.ID] = &database.ClientDocInfo{
			ServerSeq: serverSeq,
			ClientSeq: clientSeq,
			Status:    clientDocInfo.Status,
		}
		loaded.UpdatedAt = gotime.Now()
	}

	if err := updateClientInfo(ctx, txn, loaded); err != nil {
		return fmt.Errorf("update client: %w", err)
	}
	txn.Commit()

	return nil
}

// FindDeactivateCandidates finds the clients that need housekeeping.
func (d *DB) FindDeactivateCandidates(
	ctx context.Context,
	inactiveThreshold gotime.Duration,
	candidatesLimit int,
) ([]*database.ClientInfo, error) {
	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("unable to create transaction: %w", err)
	}
	defer txn.Rollback()

	var infos []*database.ClientInfo

	// updatedAt needs to be after offset.
	offset := gotime.Now().Add(-inactiveThreshold)

	// TODO(kpfaulkner) will need to check sqlite time comparrison.
	rows, err := txn.QueryContext(ctx, "SELECT * FROM ? WHERE status = ? AND updatedat > ?", tblClients, database.ClientActivated, offset)
	for rows.Next() {
		info, err := readRowIntoClientInfo(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("read row into project info: %w", err)
		}

		if info.Status != database.ClientActivated ||
			candidatesLimit <= len(infos) ||
			info.UpdatedAt.After(offset) {
			break
		}
		infos = append(infos, info)
	}

	return infos, nil
}

// FindDocInfoByKeyAndOwner finds the document of the given key. If the
// createDocIfNotExist condition is true, create the document if it does not
// exist.
func (d *DB) FindDocInfoByKeyAndOwner(
	ctx context.Context,
	projectID types.ID,
	clientID types.ID,
	key key.Key,
	createDocIfNotExist bool,
) (*database.DocInfo, error) {

	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return nil, fmt.Errorf("unable to create transaction: %w", err)
	}

	defer txn.Rollback()

	rows := txn.QueryRowContext(ctx, "SELECT * FROM ? WHERE projectid = ? AND key = ?", tblDocuments, projectID.String(), key.String())
	docInfo, err := readRowIntoDocInfo(rows.Scan)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("find document by key: %w", err)
	} else if err == sql.ErrNoRows && !createDocIfNotExist {
		return nil, fmt.Errorf("%s: %w", key, database.ErrDocumentNotFound)
	}

	noDocInfo := err == sql.ErrNoRows

	now := gotime.Now()
	if noDocInfo {
		docInfo = &database.DocInfo{
			ID:         newID(),
			ProjectID:  projectID,
			Key:        key,
			Owner:      clientID,
			ServerSeq:  0,
			CreatedAt:  now,
			AccessedAt: now,
		}

		if _, err := txn.ExecContext(ctx, "INSERT INTO ? VALUES(?, ?, ?, ?,?, ?, ?, ?)",
			tblDocuments, docInfo.ID, docInfo.ProjectID, docInfo.Key, docInfo.Owner, docInfo.ServerSeq, docInfo.CreatedAt, docInfo.AccessedAt); err != nil {
			return nil, fmt.Errorf("create document: %w", err)
		}
		txn.Commit()
	}

	return docInfo, nil
}

// FindDocInfoByKey finds the document of the given key.
func (d *DB) FindDocInfoByKey(
	ctx context.Context,
	projectID types.ID,
	key key.Key,
) (*database.DocInfo, error) {

	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("unable to create transaction: %w", err)
	}

	defer txn.Rollback()

	rows := txn.QueryRowContext(ctx, "SELECT * FROM ? WHERE projectid = ? AND key = ?", tblDocuments, projectID.String(), key.String())
	docInfo, err := readRowIntoDocInfo(rows.Scan)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%s: %w", key, database.ErrDocumentNotFound)
		}
		return nil, fmt.Errorf("find document by key: %w", err)
	}

	return docInfo, nil
}

// FindDocInfoByID finds a docInfo of the given ID.
func (d *DB) FindDocInfoByID(
	ctx context.Context,
	id types.ID,
) (*database.DocInfo, error) {

	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("unable to create transaction: %w", err)
	}

	defer txn.Rollback()

	rows := txn.QueryRowContext(ctx, "SELECT * FROM ? WHERE id = ?", tblDocuments, id.String())
	docInfo, err := readRowIntoDocInfo(rows.Scan)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%s: %w", id, database.ErrDocumentNotFound)
		}
		return nil, fmt.Errorf("find document by key: %w", err)
	}

	return docInfo, nil
}

// CreateChangeInfos stores the given changes and doc info.
func (d *DB) CreateChangeInfos(
	ctx context.Context,
	projectID types.ID,
	docInfo *database.DocInfo,
	initialServerSeq int64,
	changes []*change.Change,
) error {

	txn, err := d.conn.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return fmt.Errorf("unable to create transaction: %w", err)
	}

	defer txn.Rollback()

	for _, cn := range changes {
		encodedOperations, err := database.EncodeOperations(cn.Operations())
		if err != nil {
			return err
		}

		// how do we write the encodedOperations? ([][]byte ???) TODO(kpfaulkner) check this!
		if _, err := txn.ExecContext(ctx, "INSERT INTO ? VALUES(?, ?, ?, ?,?, ?, ?, ?)",
			tblChanges, newID(), docInfo.ID, cn.ServerSeq(), types.ID(cn.ID().ActorID().String()),
			cn.ClientSeq(), cn.ID().Lamport(), cn.Message(), encodedOperations); err != nil {
			return fmt.Errorf("create change: %w", err)
		}
	}

	rows := txn.QueryRowContext(ctx, "SELECT * FROM ? WHERE projectid = ? AND key = ?", tblDocuments, projectID.String(), docInfo.Key.String())
	loadedDocInfo, err := readRowIntoDocInfo(rows.Scan)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("%s: %w", docInfo.Key, database.ErrDocumentNotFound)
		}
		return fmt.Errorf("find document by key: %w", err)
	}

	/////////

	if loadedDocInfo.ServerSeq != initialServerSeq {
		return fmt.Errorf("%s: %w", docInfo.ID, database.ErrConflictOnUpdate)
	}

	loadedDocInfo.ServerSeq = docInfo.ServerSeq
	loadedDocInfo.UpdatedAt = gotime.Now()

	if _, err := txn.ExecContext(ctx, "UPDATE ? SET projectid=?, key=?, owner=?, serverseq=?, createdat=?, accessedat=? WHERE id=?",
		tblDocuments, loadedDocInfo.ProjectID, loadedDocInfo.Key, loadedDocInfo.Owner,
		loadedDocInfo.ServerSeq, loadedDocInfo.CreatedAt, loadedDocInfo.AccessedAt, loadedDocInfo.ID); err != nil {
		return fmt.Errorf("update document: %w", err)
	}

	txn.Commit()
	return nil
}

// PurgeStaleChanges delete changes before the smallest in `syncedseqs` to
// save storage.
func (d *DB) PurgeStaleChanges(
	ctx context.Context,
	docID types.ID,
) error {
	txn := d.db.Txn(true)
	defer txn.Abort()

	// Find the smallest server seq in `syncedseqs`.
	// Because offline client can pull changes when it becomes online.
	it, err := txn.Get(tblSyncedSeqs, "id")
	if err != nil {
		return fmt.Errorf("fetch syncedseqs: %w", err)
	}

	minSyncedServerSeq := change.MaxServerSeq
	for raw := it.Next(); raw != nil; raw = it.Next() {
		info := raw.(*database.SyncedSeqInfo)
		if info.DocID == docID && info.ServerSeq < minSyncedServerSeq {
			minSyncedServerSeq = info.ServerSeq
		}
	}
	if minSyncedServerSeq == change.MaxServerSeq {
		return nil
	}

	// Delete all changes before the smallest server seq.
	iterator, err := txn.ReverseLowerBound(
		tblChanges,
		"doc_id_server_seq",
		docID.String(),
		minSyncedServerSeq,
	)
	if err != nil {
		return fmt.Errorf("fetch changes before %d: %w", minSyncedServerSeq, err)
	}

	for raw := iterator.Next(); raw != nil; raw = iterator.Next() {
		info := raw.(*database.ChangeInfo)
		if err = txn.Delete(tblChanges, info); err != nil {
			return fmt.Errorf("delete change %s: %w", info.ID, err)
		}
	}
	return nil
}

// FindChangesBetweenServerSeqs returns the changes between two server sequences.
func (d *DB) FindChangesBetweenServerSeqs(
	ctx context.Context,
	docID types.ID,
	from int64,
	to int64,
) ([]*change.Change, error) {
	infos, err := d.FindChangeInfosBetweenServerSeqs(ctx, docID, from, to)
	if err != nil {
		return nil, err
	}

	var changes []*change.Change
	for _, info := range infos {
		c, err := info.ToChange()
		if err != nil {
			return nil, err
		}

		changes = append(changes, c)
	}

	return changes, nil
}

// FindChangeInfosBetweenServerSeqs returns the changeInfos between two server sequences.
func (d *DB) FindChangeInfosBetweenServerSeqs(
	ctx context.Context,
	docID types.ID,
	from int64,
	to int64,
) ([]*database.ChangeInfo, error) {
	txn := d.db.Txn(false)
	defer txn.Abort()

	var infos []*database.ChangeInfo

	iterator, err := txn.LowerBound(
		tblChanges,
		"doc_id_server_seq",
		docID.String(),
		from,
	)
	if err != nil {
		return nil, fmt.Errorf("fetch changes from %d: %w", from, err)
	}

	for raw := iterator.Next(); raw != nil; raw = iterator.Next() {
		info := raw.(*database.ChangeInfo)
		if info.DocID != docID || info.ServerSeq > to {
			break
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// CreateSnapshotInfo stores the snapshot of the given document.
func (d *DB) CreateSnapshotInfo(
	ctx context.Context,
	docID types.ID,
	doc *document.InternalDocument,
) error {
	snapshot, err := converter.ObjectToBytes(doc.RootObject())
	if err != nil {
		return err
	}

	txn := d.db.Txn(true)
	defer txn.Abort()

	if err := txn.Insert(tblSnapshots, &database.SnapshotInfo{
		ID:        newID(),
		DocID:     docID,
		ServerSeq: doc.Checkpoint().ServerSeq,
		Lamport:   doc.Lamport(),
		Snapshot:  snapshot,
		CreatedAt: gotime.Now(),
	}); err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}
	txn.Commit()
	return nil
}

// FindClosestSnapshotInfo finds the last snapshot of the given document.
func (d *DB) FindClosestSnapshotInfo(
	ctx context.Context,
	docID types.ID,
	serverSeq int64,
) (*database.SnapshotInfo, error) {
	txn := d.db.Txn(false)
	defer txn.Abort()

	iterator, err := txn.ReverseLowerBound(
		tblSnapshots,
		"doc_id_server_seq",
		docID.String(),
		serverSeq,
	)
	if err != nil {
		return nil, fmt.Errorf("fetch snapshots before %d: %w", serverSeq, err)
	}

	var snapshotInfo *database.SnapshotInfo
	for raw := iterator.Next(); raw != nil; raw = iterator.Next() {
		info := raw.(*database.SnapshotInfo)
		if info.DocID == docID {
			snapshotInfo = info
			break
		}
	}

	if snapshotInfo == nil {
		return &database.SnapshotInfo{}, nil
	}

	return snapshotInfo, nil
}

// FindMinSyncedSeqInfo finds the minimum synced sequence info.
func (d *DB) FindMinSyncedSeqInfo(
	ctx context.Context,
	docID types.ID,
) (*database.SyncedSeqInfo, error) {
	txn := d.db.Txn(false)
	defer txn.Abort()

	it, err := txn.Get(tblSyncedSeqs, "id")
	if err != nil {
		return nil, fmt.Errorf("fetch syncedseqs: %w", err)
	}

	syncedSeqInfo := &database.SyncedSeqInfo{}
	minSyncedServerSeq := change.MaxServerSeq
	for raw := it.Next(); raw != nil; raw = it.Next() {
		info := raw.(*database.SyncedSeqInfo)
		if info.DocID == docID && info.ServerSeq < minSyncedServerSeq {
			minSyncedServerSeq = info.ServerSeq
			syncedSeqInfo = info
		}
	}
	if minSyncedServerSeq == change.MaxServerSeq {
		return nil, nil
	}

	return syncedSeqInfo, nil
}

// UpdateAndFindMinSyncedTicket updates the given serverSeq of the given client
// and returns the min synced ticket.
func (d *DB) UpdateAndFindMinSyncedTicket(
	ctx context.Context,
	clientInfo *database.ClientInfo,
	docID types.ID,
	serverSeq int64,
) (*time.Ticket, error) {
	if err := d.UpdateSyncedSeq(ctx, clientInfo, docID, serverSeq); err != nil {
		return nil, err
	}

	txn := d.db.Txn(false)
	defer txn.Abort()

	iterator, err := txn.LowerBound(
		tblSyncedSeqs,
		"doc_id_lamport_actor_id",
		docID.String(),
		int64(0),
		time.InitialActorID.String(),
	)
	if err != nil {
		return nil, fmt.Errorf("fetch smallest syncedseq of %s: %w", docID.String(), err)
	}

	var syncedSeqInfo *database.SyncedSeqInfo
	if raw := iterator.Next(); raw != nil {
		info := raw.(*database.SyncedSeqInfo)
		if info.DocID == docID {
			syncedSeqInfo = info
		}
	}

	if syncedSeqInfo == nil || syncedSeqInfo.ServerSeq == change.InitialServerSeq {
		return time.InitialTicket, nil
	}

	actorID, err := time.ActorIDFromHex(syncedSeqInfo.ActorID.String())
	if err != nil {
		return nil, err
	}

	return time.NewTicket(
		syncedSeqInfo.Lamport,
		time.MaxDelimiter,
		actorID,
	), nil
}

// UpdateSyncedSeq updates the syncedSeq of the given client.
func (d *DB) UpdateSyncedSeq(
	ctx context.Context,
	clientInfo *database.ClientInfo,
	docID types.ID,
	serverSeq int64,
) error {
	txn := d.db.Txn(true)
	defer txn.Abort()

	isAttached, err := clientInfo.IsAttached(docID)
	if err != nil {
		return err
	}

	if !isAttached {
		if _, err = txn.DeleteAll(
			tblSyncedSeqs,
			"doc_id_client_id",
			docID.String(),
			clientInfo.ID.String(),
		); err != nil {
			return fmt.Errorf("delete syncedseqs of %s: %w", docID.String(), err)
		}
		txn.Commit()
		return nil
	}

	ticket, err := d.findTicketByServerSeq(txn, docID, serverSeq)
	if err != nil {
		return err
	}

	// NOTE: skip storing the initial ticket to prevent GC interruption.
	//       Documents in this state do not need to be saved because they do not
	//       have any tombstones to be referenced by other documents.
	//
	//       (The initial ticket is used as the creation time of the root
	//       element that operations can not remove.)
	if ticket.Compare(time.InitialTicket) == 0 {
		return nil
	}

	raw, err := txn.First(
		tblSyncedSeqs,
		"doc_id_client_id",
		docID.String(),
		clientInfo.ID.String(),
	)
	if err != nil {
		return fmt.Errorf("fetch syncedseqs of %s: %w", docID.String(), err)
	}

	syncedSeqInfo := &database.SyncedSeqInfo{
		DocID:     docID,
		ClientID:  clientInfo.ID,
		Lamport:   ticket.Lamport(),
		ActorID:   types.ID(ticket.ActorID().String()),
		ServerSeq: serverSeq,
	}
	if raw == nil {
		syncedSeqInfo.ID = newID()
	} else {
		syncedSeqInfo.ID = raw.(*database.SyncedSeqInfo).ID
	}

	if err := txn.Insert(tblSyncedSeqs, syncedSeqInfo); err != nil {
		return fmt.Errorf("insert syncedseqs of %s: %w", docID.String(), err)
	}

	txn.Commit()
	return nil
}

// FindDocInfosByPaging returns the documentInfos of the given paging.
func (d *DB) FindDocInfosByPaging(
	ctx context.Context,
	projectID types.ID,
	paging types.Paging[types.ID],
) ([]*database.DocInfo, error) {
	txn := d.db.Txn(false)
	defer txn.Abort()

	var iterator memdb.ResultIterator
	var err error
	if paging.IsForward {
		iterator, err = txn.LowerBound(
			tblDocuments,
			"project_id_id",
			projectID.String(),
			paging.Offset.String(),
		)
	} else {
		offset := paging.Offset
		if paging.Offset == "" {
			offset = types.IDFromActorID(time.MaxActorID)
		}

		iterator, err = txn.ReverseLowerBound(
			tblDocuments,
			"project_id_id",
			projectID.String(),
			offset.String(),
		)
	}
	if err != nil {
		return nil, fmt.Errorf("fetch documents of %s: %w", projectID.String(), err)
	}

	var docInfos []*database.DocInfo
	for raw := iterator.Next(); raw != nil; raw = iterator.Next() {
		info := raw.(*database.DocInfo)
		if len(docInfos) >= paging.PageSize || info.ProjectID != projectID {
			break
		}

		if info.ID != paging.Offset {
			docInfos = append(docInfos, info)
		}
	}

	return docInfos, nil
}

// FindDocInfosByQuery returns the docInfos which match the given query.
func (d *DB) FindDocInfosByQuery(
	ctx context.Context,
	projectID types.ID,
	query string,
	pageSize int,
) (*types.SearchResult[*database.DocInfo], error) {
	txn := d.db.Txn(false)
	defer txn.Abort()

	iterator, err := txn.Get(tblDocuments, "project_id_key_prefix", projectID.String(), query)
	if err != nil {
		return nil, fmt.Errorf("find docInfos by query: %w", err)
	}

	var docInfos []*database.DocInfo
	count := 0
	for raw := iterator.Next(); raw != nil; raw = iterator.Next() {
		if count < pageSize {
			info := raw.(*database.DocInfo)
			docInfos = append(docInfos, info)
		}
		count++
	}

	return &types.SearchResult[*database.DocInfo]{
		TotalCount: count,
		Elements:   docInfos,
	}, nil
}

func (d *DB) findTicketByServerSeq(
	txn *memdb.Txn,
	docID types.ID,
	serverSeq int64,
) (*time.Ticket, error) {
	if serverSeq == change.InitialServerSeq {
		return time.InitialTicket, nil
	}

	raw, err := txn.First(
		tblChanges,
		"doc_id_server_seq",
		docID.String(),
		serverSeq,
	)
	if err != nil {
		return nil, fmt.Errorf("fetch change of %s: %w", docID.String(), err)
	}
	if raw == nil {
		return nil, fmt.Errorf(
			"docID %s, serverSeq %d: %w",
			docID.String(),
			serverSeq,
			database.ErrDocumentNotFound,
		)
	}

	changeInfo := raw.(*database.ChangeInfo)
	actorID, err := time.ActorIDFromHex(changeInfo.ActorID.String())
	if err != nil {
		return nil, err
	}

	return time.NewTicket(
		changeInfo.Lamport,
		time.MaxDelimiter,
		actorID,
	), nil
}

func newID() types.ID {
	return types.ID(primitive.NewObjectID().Hex())
}
