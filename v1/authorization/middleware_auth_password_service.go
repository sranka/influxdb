package authorization

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
)

type AuthFinder interface {
	FindAuthorizationByID(ctx context.Context, id influxdb.ID) (*influxdb.Authorization, error)
}

// AuthedPasswordService is middleware for authorizing requests to the inner PasswordService.
type AuthedPasswordService struct {
	auth  AuthFinder
	inner PasswordService
}

// NewAuthedPasswordService wraps an existing PasswordService with authorization middleware.
func NewAuthedPasswordService(auth AuthFinder, inner PasswordService) *AuthedPasswordService {
	return &AuthedPasswordService{auth: auth, inner: inner}
}

// SetPassword overrides the password of a known user.
func (s *AuthedPasswordService) SetPassword(ctx context.Context, authID influxdb.ID, password string) error {
	auth, err := s.auth.FindAuthorizationByID(ctx, authID)
	if err != nil {
		return ErrAuthNotFound
	}

	if _, _, err := authorizer.AuthorizeWriteResource(ctx, influxdb.UsersResourceType, auth.UserID); err != nil {
		return err
	}

	return s.inner.SetPassword(ctx, authID, password)
}

// SetPasswordHash overrides the password hash of a known user.
func (s *AuthedPasswordService) SetPasswordHash(ctx context.Context, authID influxdb.ID, password string) error {
	auth, err := s.auth.FindAuthorizationByID(ctx, authID)
	if err != nil {
		return ErrAuthNotFound
	}

	if _, _, err := authorizer.AuthorizeWriteResource(ctx, influxdb.UsersResourceType, auth.UserID); err != nil {
		return err
	}

	return s.inner.SetPasswordHash(ctx, authID, password)
}
