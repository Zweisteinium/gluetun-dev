package portforward

import (
	"context"
	"errors"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/qdm12/gluetun/internal/configuration/settings"
	"github.com/qdm12/gluetun/internal/constants/providers"
	"github.com/qdm12/gluetun/internal/netlink"
	"github.com/qdm12/gluetun/internal/portforward/service"
	"github.com/qdm12/gluetun/internal/provider/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func Test_Loop_failedFirstPortForward(t *testing.T) {
	t.Parallel()

	testCases := map[string]struct {
		updateAgain bool // instead of letting the retry timer fire
	}{
		"retried":                         {},
		"pending_retry_dropped_by_update": {updateAgain: true},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)

			const vpnInterface = "tun0"
			const forwardedPort = uint16(45066)
			gateway := netip.MustParseAddr("10.2.0.1")
			internalIP := netip.MustParseAddr("10.2.0.2")
			portFilepath := filepath.Join(t.TempDir(), "forwarded_port")
			client := &http.Client{}

			routing := NewMockRouting(ctrl)
			routing.EXPECT().VPNLocalGatewayIP(vpnInterface).Return(gateway, nil).Times(2)
			routing.EXPECT().AssignedIP(vpnInterface, netlink.FamilyV4).Return(internalIP, nil).Times(2)
			logger := NewMockLogger(ctrl)
			portAllower := NewMockPortAllower(ctrl)
			portForwarder := NewMockPortForwarder(ctrl)
			retryTimer := NewMocktimer(ctrl)
			objects := utils.PortForwardObjects{
				Logger:     logger,
				Gateway:    gateway,
				InternalIP: internalIP,
				Client:     client,
				PortsCount: 1,
			}

			// Read once per select: on the two starts and on the loop exit
			retryCh := make(chan time.Time)
			retryTimer.EXPECT().C().Return(retryCh).Times(3)

			// The first port forwarding fails and arms the retry timer. Whatever
			// restarts the service next stops it, so a pending retry cannot restart
			// it again. The second port forwarding succeeds and is kept until the
			// loop stops.
			logger.EXPECT().Info("starting").Times(2)
			gomock.InOrder(
				retryTimer.EXPECT().Stop(),
				portForwarder.EXPECT().PortForward(gomock.Any(), objects).
					Return(nil, errors.New("TCP and UDP external ports differ: 45066 and 60808")),
				retryTimer.EXPECT().Reset(5*time.Second),
				retryTimer.EXPECT().Stop(),
				portForwarder.EXPECT().PortForward(gomock.Any(), objects).
					Return(map[uint16]uint16{forwardedPort: forwardedPort}, nil),
			)
			logger.EXPECT().Info("port forwarded is 45066")
			portAllower.EXPECT().SetAllowedPort(gomock.Any(), forwardedPort, vpnInterface).Return(nil)
			logger.EXPECT().Info("writing port file " + portFilepath)
			keeping := make(chan struct{})
			portForwarder.EXPECT().KeepPortForward(gomock.Any(), objects).
				DoAndReturn(func(ctx context.Context, _ utils.PortForwardObjects) error {
					close(keeping)
					<-ctx.Done()
					return ctx.Err()
				})
			logger.EXPECT().Info("stopping")
			portAllower.EXPECT().RemoveAllowedPort(context.Background(), forwardedPort).Return(nil)
			logger.EXPECT().Info("clearing port file " + portFilepath)

			loop := NewLoop(settings.PortForwarding{
				Enabled:        new(true),
				Filepath:       &portFilepath,
				UpCommand:      new(""),
				DownCommand:    new(""),
				ListeningPorts: []uint16{0},
				PortsCount:     1,
			}, routing, client, portAllower, logger, nil, os.Getuid(), os.Getgid())
			loop.newTimer = func() timer { return retryTimer }
			_, err := loop.Start(t.Context())
			require.NoError(t, err)

			vpnUp := Settings{
				VPNIsUp: new(true),
				Service: service.Settings{
					PortForwarder: portForwarder,
					Interface:     vpnInterface,
				},
			}
			err = loop.UpdateWith(vpnUp)
			require.ErrorContains(t, err, "TCP and UDP external ports differ")
			assert.ErrorContains(t, err, "retrying in 5s")

			if testCase.updateAgain {
				portForwarder.EXPECT().Name().Return(providers.Protonvpn) // settings validation
				err = loop.UpdateWith(vpnUp)
				require.NoError(t, err)
			} else {
				retryCh <- time.Time{}
			}

			<-keeping
			assert.Equal(t, []uint16{forwardedPort}, loop.GetPortsForwarded())

			err = loop.Stop()
			require.NoError(t, err)
		})
	}
}
