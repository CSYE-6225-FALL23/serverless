const axios = require("axios");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { PutCommand, DynamoDBDocumentClient } = require("@aws-sdk/lib-dynamodb");
const mailgun = require("mailgun-js");

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);
const { Storage } = require("@google-cloud/storage");

exports.handler = async (event, context) => {
  try {
    let submissionStatus = false;
    let gcsStatus = false;
    let emailStatus = false;

    const snsMessage = JSON.parse(event.Records[0].Sns.Message);
    const { submissionUrl, email, userId, submissionId, assignmentId } =
      snsMessage;

    const file = await downloadSubmissionZip(submissionUrl);
    if (!file) {
      emailStatus = await sendEmailToUser(email, "fail");
    } else {
      submissionStatus = true;

      [submissionStatus, emailStatus] = await Promise.all([
        uploadToGCS(file, assignmentId, userId),
        sendEmailToUser(email, "success"),
      ]);
    }

    console.info(
      "Status: ",
      JSON.stringify({
        submissionStatus: submissionStatus,
        gcsStatus: submissionStatus,
        emailStatus: emailStatus,
      }),
    );

    await createEventInDynamoDB({
      userId: userId,
      email: email,
      assignmentId: assignmentId,
      submissionId: submissionId,
      submissionStatus: submissionStatus,
      gcsStatus: gcsStatus,
      emailStatus: emailStatus,
    });
  } catch (error) {
    console.error("Error in one of the lambda steps:", error);
  }
};

const downloadSubmissionZip = async (submissionUrl) => {
  try {
    const response = await axios.get(submissionUrl, { responseType: "stream" });
    console.info(
      "Downloaded zip file from URL",
      JSON.stringify({
        status: response.status,
        url: response.config.url,
      }),
    );
    return response;
  } catch (err) {
    console.error("Error downloading zip", err);
    return false;
  }
};

const uploadToGCS = async (submissionFile, assignmentId, userId) => {
  console.info("Uploading zip to GCS...");
  try {
    // Create a GCS client
    const serviceAccountKey = JSON.parse(atob(process.env.GCP_SERVICE_ACCOUNT_PVT_KEY));
    const storage = new Storage({ credentials: serviceAccountKey });
    // const storage = new Storage({ keyFilename: "./accesskeys.json" });

    // GitHub and GCS information
    const gcsFileName = `${assignmentId}/${userId}.zip`;
    const gcsBucketName = process.env.GCS_BUCKET_NAME;

    // Upload the file to GCS
    const bucket = storage.bucket(gcsBucketName);
    const file = bucket.file(gcsFileName);
    const writeStream = file.createWriteStream();

    submissionFile.data.pipe(writeStream);

    return new Promise((resolve, reject) => {
      writeStream.on("error", (err) => {
        console.error("Error writing to bucket", err);
        resolve(false);
      });

      writeStream.on("finish", () => {
        console.info("Finished uploading to bucket");
        resolve(true);
      });
    });
  } catch (error) {
    console.error("Error uploading zip to bucket", error);
    return false;
  }
};

const createEventInDynamoDB = async (snsMessage) => {
  console.log("Inserting event to DynamoDB", JSON.stringify(snsMessage));
  try {
    const params = new PutCommand({
      TableName: process.env.DYNAMODB_TABLE_NAME,
      Item: {
        id: snsMessage.submissionId,
        userId: snsMessage.userId,
        email: snsMessage.email,
        assignmentId: snsMessage.assignmentId,
        submissionStatus: snsMessage.submissionStatus,
        gcsStatus: snsMessage.gcsStatus,
        emailStatus: snsMessage.emailStatus,
        timestamp: Date.now(),
      },
    });

    return docClient
      .send(params)
      .then((data) => {
        console.info("Inserted email event to DynamoDB:", JSON.stringify(data));
        return true;
      })
      .catch((err) => {
        console.error("Error inserting email event to DynamoDB:", err);
        return false;
      });
  } catch (err) {
    console.error("Error inserting email event to DynamoDB:", err);
    return false;
  }
};

const sendEmailToUser = async (email, type) => {
  // Replace these with your Mailgun API key and domain
  const apiKey = process.env.EMAIL_API_KEY;
  const domain = process.env.EMAIL_DOMAIN;

  // Create a Mailgun instance with your API key and domain
  const mg = mailgun({ apiKey, domain });

  // Define the email data
  let data;
  if (type === "success") {
    data = {
      from: "CSYE6225 Submission notifications@skudli.xyz",
      to: email,
      subject: "Assignment submission accecpted",
      text: "Your submission was successfully received and verified. Thank you.",
    };
  } else if (type === "fail") {
    data = {
      from: "CSYE6225 Submission notifications@skudli.xyz",
      to: email,
      subject: "Assignment submission failed",
      text: "Your submission could not be downloaded. Please verify the URL and resubmit.",
    };
  }

  // Send the email
  return mg
    .messages()
    .send(data)
    .then(() => {
      console.info("Email sent to", email);
      return true;
    })
    .catch((err) => {
      console.error("Error sending email to", email);
      console.error("Email failed: ", err);
      return true;
    });
};
