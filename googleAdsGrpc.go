package main

import (
	"context"
	"fmt"
	"time"

	"github.com/shenzhencenter/google-ads-pb/clients"
)

const (
	developerToken      = "DEVELOPER_TOKEN"
	clientID            = "CLIENT_ID"
	clientSecret        = "CLIENT_SECRET"
	refreshToken        = "REFRESH_TOKEN"
	googleAdsAPIBase    = "https://googleads.googleapis.com"
	googleAdsAPIVersion = "v13"
	dynamoDBTableName   = "GoogleAdsMetrics"
)

type GoogleAdsClient struct {
	client *clients.GoogleAdsClient
}

func NewGoogleAdsClient(ctx context.Context) (*GoogleAdsClient, error) {
	client, err := clients.NewGoogleAdsClient(ctx)
	if err != nil {
		return nil, err
	}
	return &GoogleAdsClient{client: client}, nil
}

// func (g *GoogleAdsClient) FetchMetrics(ctx context.Context, query string) ([]*servicespb.GoogleAdsRow, error) {
// 	req := &servicespb.SearchGoogleAdsRequest{
// 		CustomerId:              customerID,
// 		Query:                   query,
// 		ReturnTotalResultsCount: true,
// 	}

// 	var rows []*servicespb.GoogleAdsRow

// 	for {
// 		stream := g.client.Search(ctx, req)

// 		for {
// 			row, err := stream.Next()
// 			if err == iterator.Done {
// 				break
// 			}
// 			if err != nil {
// 				return nil, err
// 			}

// 			rows = append(rows, row)
// 		}

// 		if req.PageToken == "" {
// 			break
// 		}
// 	}

// 	return rows, nil
// }

// type DynamoDBWriter struct {
// 	dynamoDBClient *dynamodb.DynamoDB
// }

// func NewDynamoDBWriter() (*DynamoDBWriter, error) {
// 	//sess, err := session.NewSession(&aws.Config{
// 	//	Region: aws.String(os.Getenv("AWS_REGION")),
// 	//})
// 	sess, err := session.NewSession(&aws.Config{
// 		Region: aws.String("eu-west-2"),
// 	})
// 	if err != nil {
// 		return nil, err
// 	}

// 	dynamoDBClient := dynamodb.New(sess)
// 	return &DynamoDBWriter{dynamoDBClient: dynamoDBClient}, nil
// }

type MetricItem struct {
	// Add necessary fields based on the GAQL query
	Email string `json:"email"`
	Key   string `json:"key"`
}

// func (d *DynamoDBWriter) SaveMetrics(ctx context.Context, metrics []Row) error {
// 	for _, metric := range metrics {

// 		fmt.Println(metric)
// 		item, err := dynamodbattribute.MarshalMap(metric)
// 		fmt.Println(item)
// 		if err != nil {
// 			return err
// 		}

// 		input := &dynamodb.PutItemInput{
// 			Item:      item,
// 			TableName: aws.String("therealyo--image-uploader-dynamodb"),
// 		}

// 		_, err = d.dynamoDBClient.PutItem(input)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

func buildGAQLQuery(startDate, endDate string) string {
	query := fmt.Sprintf(`
		SELECT 
		campaign.id,
		ad_group.id,
		ad_group_ad.ad.id,
		campaign.name,
		campaign.optimization_score,
		campaign.serving_status,
		campaign_budget.amount_micros,
		ad_group_ad.ad.name,
		ad_group_ad.ad_strength,
		ad_group_ad.status,
		ad_group.name,
		ad_group.target_roas,
		campaign.target_roas.target_roas,
		campaign_group.id,
		campaign_group.name,
		customer.id,
		customer.manager,
		customer.descriptive_name,
		customer.currency_code,
		customer.time_zone,
		segments.date,
		metrics.active_view_cpm,
		metrics.active_view_ctr,
		metrics.active_view_impressions,
		metrics.active_view_measurable_impressions,
		metrics.active_view_measurable_cost_micros,
		metrics.active_view_measurability,
		metrics.active_view_viewability,
		metrics.all_conversions,
		metrics.all_conversions_value,
		metrics.average_cost,
		metrics.average_cpc,
		metrics.average_cpe,
		metrics.average_cpm,
		metrics.average_cpv,
		metrics.content_budget_lost_impression_share,
		metrics.content_rank_lost_impression_share,
		metrics.clicks,
		metrics.conversions,
		metrics.conversions_value,
		metrics.cost_micros,
		metrics.cost_per_all_conversions,
		metrics.cost_per_conversion,
		metrics.ctr,
		metrics.engagement_rate,
		metrics.engagements,
		metrics.impressions,
		metrics.interactions,
		metrics.interaction_rate,
		metrics.top_impression_percentage,
		metrics.invalid_clicks,
		metrics.value_per_all_conversions,
		metrics.value_per_conversion,
		metrics.video_views,
		metrics.video_view_rate,
		metrics.video_quartile_p100_rate,
		metrics.video_quartile_p75_rate,
		metrics.video_quartile_p50_rate,
		metrics.video_quartile_p25_rate,
		metrics.view_through_conversions,
		metrics.relative_ctr
		FROM campaign
		INNER JOIN ad_group ON campaign.id = ad_group.campaign.id
		INNER JOIN ad_group_ad ON ad_group.id = ad_group_ad.ad_group.id
		WHERE campaign.status != 'REMOVED' AND segments.date BETWEEN '%s' AND '%s'
		ORDER BY campaign.id, ad_group.id, ad_group_ad.ad.id
		LIMIT 1000
	`, startDate, endDate)
	return query
}

// func main() {
// 	//lambda.Start(HandleRequest)

// 	fmt.Println("here")
// 	dynamo, err := NewDynamoDBWriter()
// 	if err != nil {
// 		fmt.Println(err)
// 	}

// 	fmt.Println(dynamo.dynamoDBClient)

// 	mock := []MetricItem{
// 		{Email: "test1@gmail.com", Key: "test1"},
// 		{Email: "test2@gmail.com", Key: "test2"},
// 		{Email: "test3@gmail.com", Key: "test3"},
// 	}

// 	err = dynamo.SaveMetrics(context.Background(), mock)
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	// err := HandleRequest(context.Background(), "557-474-3054")
// 	// if err != nil {
// 	// 	fmt.Println(err)
// 	// }
// }

// func HandleRequest(ctx context.Context, event events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
// 	startDate := time.Now().AddDate(0, 0, -28).Format("2006-01-02")
// 	endDate := time.Now().Format("2006-01-02")
// 	query := buildGAQLQuery(startDate, endDate)

// 	googleAdsClient, err := NewGoogleAdsClient(ctx, loginCustID)
// 	if err != nil {
// 		return events.APIGatewayProxyResponse{}, err
// 	}

// 	metrics, err := googleAdsClient.FetchMetrics(ctx, query)
// 	if err != nil {
// 		return events.APIGatewayProxyResponse{}, err
// 	}

// 	dynamoDBWriter, err := NewDynamoDBWriter()
// 	if err != nil {
// 		return events.APIGatewayProxyResponse{}, err
// 	}

// 	metricItems := convertMetricsToItems(metrics) // Implement this function to convert []*googleads.GoogleAdsRow to []MetricItem
// 	err = dynamoDBWriter.SaveMetrics(ctx, metricItems)
// 	if err != nil {
// 		return events.APIGatewayProxyResponse{}, err
// 	}

// 	return events.APIGatewayProxyResponse{
// 		Body:       "Successfully saved metrics to DynamoDB",
// 		StatusCode: 200,
// 	}, nil
// }

func HandleRequest(ctx context.Context, loginCustID string) error {
	startDate := time.Now().AddDate(0, 0, -28).Format("2006-01-02")
	endDate := time.Now().Format("2006-01-02")
	query := buildGAQLQuery(startDate, endDate)

	// ctx := context.Background()

	// headers := metadata.Pairs(
	// 	"authorization", "Bearer "+os.Getenv("ACCESS_TOKEN"),
	// 	"developer-token", os.Getenv("DEVELOPER_TOKEN"),
	// 	"login-customer-id", os.Getenv("CUSTOMER_ID"),
	// )
	// ctx = metadata.NewOutgoingContext(ctx, headers)
	googleAdsClient, err := NewGoogleAdsClient(ctx)
	if err != nil {
		return err
	}

	metrics, err := googleAdsClient.FetchMetrics(ctx, query)
	if err != nil {
		return err
	}

	fmt.Println(metrics)

	dynamoDBWriter, err := NewDynamoDBWriter()
	if err != nil {
		return err
	}

	metricItems := convertMetricsToItems(metrics) // Implement this function to convert []*googleads.GoogleAdsRow to []MetricItem
	err = dynamoDBWriter.SaveMetrics(ctx, metricItems)
	if err != nil {
		return err
	}

	return nil
}

// Exponential backoff retry function
// package main

// import (
// 	"context"
// 	"fmt"
// 	"math/rand"
// 	"time"

// 	// Other imports remain the same
// )

// // Other parts of the code remain the same

// func (g *GoogleAdsClient) FetchMetrics(ctx context.Context, query string) ([]*googleads.GoogleAdsRow, error) {
// 	req := &googleads.SearchGoogleAdsRequest{
// 		CustomerId:               loginCustID,
// 		Query:                    query,
// 		ReturnTotalResultsCount:  true,
// 	}

// 	var rows []*googleads.GoogleAdsRow
// 	var err error

// 	backoff := func(retries int) time.Duration {
// 		return time.Duration(rand.Intn(1<<retries)) * time.Second
// 	}

// 	maxRetries := 5
// 	for i := 0; i < maxRetries; i++ {
// 		rows, err = g.fetchMetricsWithRetry(ctx, req)
// 		if err == nil {
// 			break
// 		}

// 		select {
// 		case <-time.After(backoff(i)):
// 		case <-ctx.Done():
// 			return nil, ctx.Err()
// 		}
// 	}

// 	if err != nil {
// 		return nil, err
// 	}

// 	return rows, nil
// }

// func (g *GoogleAdsClient) fetchMetricsWithRetry(ctx context.Context, req *googleads.SearchGoogleAdsRequest) ([]*googleads.GoogleAdsRow, error) {
// 	var rows []*googleads.GoogleAdsRow

// 	for {
// 		stream, err := g.client.Search(ctx, req)
// 		if err != nil {
// 			return nil, err
// 		}

// 		for {
// 			row, err := stream.Recv()
// 			if err == io.EOF {
// 				break
// 			}
// 			if err != nil {
// 				return nil, err
// 			}

// 			rows = append(rows, row)
// 		}

// 		if req.PageToken == "" {
// 			break
// 		}
// 	}

// 	return rows, nil
// }
