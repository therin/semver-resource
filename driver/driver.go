package driver

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/blang/semver"
	"github.com/concourse/semver-resource/models"
	"github.com/concourse/semver-resource/version"
)

type Driver interface {
	Bump(version.Bump) (semver.Version, error)
	Set(semver.Version) error
	Check(*semver.Version) ([]semver.Version, error)
}

const maxRetries = 12

func FromSource(source models.Source) (Driver, error) {
	var initialVersion semver.Version
	if source.InitialVersion != "" {
		version, err := semver.Parse(source.InitialVersion)
		if err != nil {
			return nil, fmt.Errorf("invalid initial version (%s): %s", source.InitialVersion, err)
		}

		initialVersion = version
	} else {
		initialVersion = semver.Version{Major: 0, Minor: 0, Patch: 0}
	}

	switch source.Driver {
	case models.DriverUnspecified, models.DriverS3:
		var creds *credentials.Credentials

		sess := session.Must(session.NewSession())

		if source.AccessKeyID == "" && source.SecretAccessKey == "" {
			if source.RoleArn == "" {
				creds = credentials.AnonymousCredentials
			} else {
				// Initial credentials loaded from EC2 instance
				// role. These credentials will be used to make the STS Assume Role API.

				creds = credentials.NewCredentials(
					&ec2rolecreds.EC2RoleProvider{
						Client: ec2metadata.New(session.New()),
					},
				)
				_, err := creds.Get()
				// If unsuccessful fall back to anonymous
				if err != nil {
					creds = credentials.AnonymousCredentials
				} else {
					creds = credentials.NewStaticCredentials(source.AccessKeyID, source.SecretAccessKey, "")
				}

				// Create the credentials from AssumeRoleProvider to assume the role
				// referenced by RoleArn.
				creds = stscreds.NewCredentials(sess, source.RoleArn)

			}
		} else {
			// Use provided AWS keys
			creds = credentials.NewStaticCredentials(source.AccessKeyID, source.SecretAccessKey, "")
		}

		regionName := source.RegionName
		if len(regionName) == 0 {
			regionName = "us-east-1"
		}

		awsConfig := &aws.Config{
			Region:           aws.String(regionName),
			Credentials:      creds,
			S3ForcePathStyle: aws.Bool(true),
			MaxRetries:       aws.Int(maxRetries),
			DisableSSL:       aws.Bool(source.DisableSSL),
		}

		if len(source.Endpoint) != 0 {
			awsConfig.Endpoint = aws.String(source.Endpoint)
		}

		svc := s3.New(sess, awsConfig)
		// Create service client value configured for credentials
		// from assumed role.
		// svc := s3.New(sess, &aws.Config{Credentials: creds})

		return &S3Driver{
			InitialVersion:       initialVersion,
			Svc:                  svc,
			BucketName:           source.Bucket,
			Key:                  source.Key,
			ServerSideEncryption: source.ServerSideEncryption,
		}, nil
	case models.DriverGit:
		return &GitDriver{
			InitialVersion: initialVersion,

			URI:        source.URI,
			Branch:     source.Branch,
			PrivateKey: source.PrivateKey,
			Username:   source.Username,
			Password:   source.Password,
			File:       source.File,
			GitUser:    source.GitUser,
		}, nil

	case models.DriverSwift:
		return NewSwiftDriver(&source)

	case models.DriverGCS:
		servicer := &GCSIOServicer{
			JSONCredentials: source.JSONKey,
		}

		return &GCSDriver{
			InitialVersion: initialVersion,

			Servicer:   servicer,
			BucketName: source.Bucket,
			Key:        source.Key,
		}, nil

	default:
		return nil, fmt.Errorf("unknown driver: %s", source.Driver)
	}
}
