import {ApolloClient} from '@apollo/client';

import {PIPELINE_RUN_LOGS_SUBSCRIPTION} from '../../runs/PipelineRunLogsSubscription';
import {PipelineRunLogsSubscription} from '../../runs/types/PipelineRunLogsSubscription';

export type MessageType = InitializeType;
type InitializeType = {
  type: 'INITIALIZE';
  runId: string;
  postMessage: (data: any) => void;
  getApolloClient: () => ApolloClient<any>;
  staticPathRoot: string;
  rootServerURI: string;
};

export function onMainThreadMessage(data: MessageType) {
  switch (data.type) {
    case 'INITIALIZE':
      initialize(data); // subscribes to the data using data argument
      break;
  }
}

function initialize(data: MessageType) {
  const postMessage = data.postMessage;
  data
    .getApolloClient()
    .subscribe({
      query: PIPELINE_RUN_LOGS_SUBSCRIPTION,
      fetchPolicy: 'no-cache',
      variables: {runId: data.runId},
    })
    .subscribe({
      next({data}: {data: PipelineRunLogsSubscription}) {
        const logs = data?.pipelineRunLogs;
        if (!logs || logs.__typename === 'PipelineRunLogsSubscriptionFailure') {
          console.error('PipelineRunLogsSubscriptionFailure', logs);
          return;
        }
        postMessage(logs);
      },
      error(error: any) {
        console.error(error);
      },
    });
}
