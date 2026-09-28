package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/gagliardetto/solana-go/rpc"
	jito_pb "github.com/weeaa/jito-go/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

type Counts struct {
	sent     int
	landed   int
	rejected int
}

var stats = Counts{
	sent:     0,
	landed:   0,
	rejected: 0,
}

func main() {
	// Parse command-line arguments
	keypairPath := flag.String("keypair", "", "path to keypair file (required)")
	blockEngineUrl := flag.String("engine", "ny.mainnet.block-engine.jito.wtf:443", "block engine URL")
	rpcUrl := flag.String("rpc", "https://api.mainnet-beta.solana.com", "RPC URL")
	tipLamports := flag.Uint64("tip-lamports", 1000, "tip amount in lamports")
	numBundles := flag.Int("bundles", 20, "number of bundles to send")
	flag.Parse()

	if *keypairPath == "" {
		flag.Usage()
		return
	}

	fmt.Println("jito searcher bot starting...")

	// load the keypair file (this is a solana-keygen json file, so let solana-go parse it)
	keyData, err := ioutil.ReadFile(*keypairPath)
	if err != nil {
		fmt.Println("error reading keypair file:", err)
		return
	}

	keypair, err := solana.PrivateKeyFromSolanaKeygenFileBytes(keyData)
	if err != nil {
		fmt.Println("error parsing keypair file:", err)
		return
	}
	fmt.Printf("loaded keypair: %s\n", keypair.PublicKey().String())

	// connect to the block engine
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")),
	}
	conn, err := grpc.Dial(*blockEngineUrl, opts...)
	if err != nil {
		fmt.Println("failed to connect to block engine:", err)
		return
	}
	defer conn.Close()

	// jito requires a signed challenge before it'll let us send bundles
	accessToken, err := authenticate(context.Background(), conn, keypair)
	if err != nil {
		fmt.Println("failed to authenticate:", err)
		return
	}
	fmt.Println("authenticated with block engine")

	searcherClient := jito_pb.NewSearcherServiceClient(conn)

	// subscribe to bundle results
	go listenForResults(searcherClient, accessToken)

	// rpc client for transactions
	rpcClient := rpc.New(*rpcUrl)

	// ask the block engine for a tip account
	tipAccountsResp, err := searcherClient.GetTipAccounts(context.Background(), &jito_pb.GetTipAccountsRequest{})
	if err != nil {
		fmt.Println("error getting tip accounts:", err)
		return
	}
	if len(tipAccountsResp.Accounts) == 0 {
		fmt.Println("block engine gave us no tip accounts")
		return
	}
	tip_account := solana.MustPublicKeyFromBase58(tipAccountsResp.Accounts[0])
	fmt.Printf("using tip account: %s\n", tip_account.String())

	// fire off the bundles one by one
	for bundle_num := 0; bundle_num < *numBundles; bundle_num++ {
		ctx := context.Background()

		// get recent blockhash
		recent, err := rpcClient.GetRecentBlockhash(ctx, rpc.CommitmentProcessed)
		if err != nil {
			fmt.Println("error getting blockhash:", err)
			continue
		}

		// build a simple bundle with a tip transaction
		recent_hash := recent.Value.Blockhash

		// create transfer instruction for tip
		tip_ix := system.NewTransferInstruction(
			*tipLamports,
			keypair.PublicKey(),
			tip_account,
		).Build()

		tx, err := solana.NewTransaction(
			[]solana.Instruction{tip_ix},
			recent_hash,
			solana.TransactionPayer(keypair.PublicKey()),
		)
		if err != nil {
			fmt.Println("error creating tx:", err)
			continue
		}

		// sign the transaction
		_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
			if key.Equals(keypair.PublicKey()) {
				return &keypair
			}
			return nil
		})
		if err != nil {
			fmt.Println("error signing tx:", err)
			continue
		}

		txBytes, err := tx.MarshalBinary()
		if err != nil {
			fmt.Println("error encoding tx:", err)
			continue
		}

		// submit the bundle
		bundle := &jito_pb.Bundle{
			Packets: []*jito_pb.Packet{
				{
					Data: txBytes,
					Meta: &jito_pb.Meta{Size: uint64(len(txBytes))},
				},
			},
		}

		bundle_ctx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+accessToken)

		resp, err := searcherClient.SendBundle(bundle_ctx, &jito_pb.SendBundleRequest{Bundle: bundle})
		if err != nil {
			fmt.Println("error sending bundle:", err)
			continue
		}

		stats.sent++

		fmt.Printf("bundle %d sent: %s\n", bundle_num+1, resp.Uuid)

		time.Sleep(1 * time.Second)
	}

	// give the results stream time to catch up
	time.Sleep(10 * time.Second)

	// print land rate stats
	fmt.Println("done.")
	fmt.Printf("sent: %d\n", stats.sent)
	fmt.Printf("landed: %d\n", stats.landed)
	fmt.Printf("rejected: %d\n", stats.rejected)

	if stats.sent > 0 {
		rate := float64(stats.landed) / float64(stats.sent) * 100
		fmt.Printf("land rate: %.2f%%\n", rate)
	}
}

// authenticate does the challenge/response dance jito wants: ask for a challenge,
// login to the block engine and get an access token
func authenticate(ctx context.Context, conn *grpc.ClientConn, keypair solana.PrivateKey) (string, error) {
	authClient := jito_pb.NewAuthServiceClient(conn)

	challengeResp, err := authClient.GenerateAuthChallenge(ctx, &jito_pb.GenerateAuthChallengeRequest{
		Role:   jito_pb.Role_SEARCHER,
		Pubkey: keypair.PublicKey().Bytes(),
	})
	if err != nil {
		return "", err
	}

	challenge := fmt.Sprintf("%s-%s", keypair.PublicKey().String(), challengeResp.Challenge)

	signature, err := keypair.Sign([]byte(challenge))
	if err != nil {
		return "", err
	}

	tokensResp, err := authClient.GenerateAuthTokens(ctx, &jito_pb.GenerateAuthTokensRequest{
		Challenge:       challenge,
		ClientPubkey:    keypair.PublicKey().Bytes(),
		SignedChallenge: signature[:],
	})
	if err != nil {
		return "", err
	}

	return tokensResp.AccessToken.Value, nil
}

func listenForResults(client jito_pb.SearcherServiceClient, accessToken string) {
	fmt.Println("subscribed, waiting...")

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+accessToken)

	stream, err := client.SubscribeBundleResults(ctx, &jito_pb.SubscribeBundleResultsRequest{})
	if err != nil {
		fmt.Println("error subscribing:", err)
		return
	}

	for {
		result, err := stream.Recv()
		if err != nil {
			fmt.Println("error receiving bundle result:", err)
			return
		}

		if result == nil {
			continue
		}

		bundle_id_str := result.GetBundleId()

		if processed := result.GetProcessed(); processed != nil {
			stats.landed++
			fmt.Printf("LANDED: %s (slot: %d)\n", bundle_id_str, processed.Slot)
		} else if finalized := result.GetFinalized(); finalized != nil {
			stats.landed++
			fmt.Printf("LANDED: %s\n", bundle_id_str)
		} else if rejected := result.GetRejected(); rejected != nil {
			stats.rejected++
			fmt.Printf("rejected: %s\n", bundle_id_str)
		} else if dropped := result.GetDropped(); dropped != nil {
			stats.rejected++
			fmt.Printf("dropped: %s\n", bundle_id_str)
		} else {
			fmt.Printf("bundle update: %s\n", bundle_id_str)
		}
	}
}
