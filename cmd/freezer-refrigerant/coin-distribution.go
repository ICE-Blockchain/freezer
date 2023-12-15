// SPDX-License-Identifier: ice License 1.0

package main

import (
	"context"

	"github.com/pkg/errors"

	"github.com/ice-blockchain/wintr/server"
)

func (s *service) setupCoinDistributionRoutes(router *server.Router) {
	router.
		Group("/v1w").
		POST("/getCoinDistributionsForReview", server.RootHandler(s.GetCoinDistributionsForReview))
}

// GetCoinDistributionsForReview godoc
//
//	@Schemes
//	@Description	Fetches data of pending coin distributions for review.
//	@Tags			CoinDistribution
//	@Accept			json
//	@Produce		json
//	@Param			Authorization	header		string	true	"Insert your access token"	default(Bearer <Add access token here>)
//	@Param			x_client_type	query		string	false	"the type of the client calling this API. I.E. `web`"
//	@Param			cursor			query		uint64	true	"current cursor to fetch data from" default(0)
//	@Param			limit			query		uint64	false	"count of records in response, 5000 by default"
//	@Success		200				{object}	CoinDistributionsForReview
//	@Failure		401				{object}	server.ErrorResponse	"if not authorized"
//	@Failure		403				{object}	server.ErrorResponse	"if not allowed"
//	@Failure		422				{object}	server.ErrorResponse	"if syntax fails"
//	@Failure		500				{object}	server.ErrorResponse
//	@Failure		504				{object}	server.ErrorResponse	"if request times out"
//	@Router			/getCoinDistributionsForReview [POST].
func (s *service) GetCoinDistributionsForReview( //nolint:gocritic // .
	ctx context.Context,
	req *server.Request[GetCoinDistributionForReviewParams, CoinDistributionsForReview],
) (*server.Response[CoinDistributionsForReview], *server.Response[server.ErrorResponse]) {
	if req.AuthenticatedUser.Role != adminRole {
		return nil, server.Forbidden(errors.Errorf("insufficient role: %v, admin role required", req.AuthenticatedUser.Role))
	}
	if req.Data.Limit == 0 {
		req.Data.Limit = defaultDistributionLimit
	}
	cursor, distributions, err := s.coinDistributionRepository.GetCoinDistributionsForReview(ctx, req.Data.Cursor, req.Data.Limit)
	if err != nil {
		return nil, server.Unexpected(errors.Wrapf(err, "failed to fetch %v records from review coin distribution starting with %v", req.Data.Limit, req.Data.Cursor))
	}

	return server.OK(&CoinDistributionsForReview{
		Cursor:        cursor,
		Distributions: distributions,
	}), nil
}
