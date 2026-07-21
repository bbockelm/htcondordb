const path = require('path');
const CopyWebpackPlugin = require('copy-webpack-plugin');

const pluginId = 'bbockelm-htcondordb-datasource';

// Self-contained Grafana plugin frontend build: emit an AMD module (Grafana loads
// it via SystemJS) with React and all @grafana/* provided by the host at runtime.
module.exports = (env, argv = {}) => ({
  target: 'web',
  context: path.join(process.cwd(), 'src'),
  entry: { module: './module.ts' },
  output: {
    path: path.resolve(process.cwd(), 'dist'),
    filename: '[name].js',
    library: { type: 'amd' },
    publicPath: `public/plugins/${pluginId}/`,
    uniqueName: pluginId,
    clean: true,
  },
  externals: ['react', 'react-dom', 'rxjs', 'lodash', /^@grafana\/.+$/, /^@emotion\/.+$/],
  resolve: { extensions: ['.ts', '.tsx', '.js', '.jsx'] },
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        exclude: /node_modules/,
        use: { loader: 'ts-loader', options: { transpileOnly: true } },
      },
    ],
  },
  plugins: [
    new CopyWebpackPlugin({
      patterns: [
        { from: 'plugin.json', to: '.' },
        { from: 'img', to: 'img', noErrorOnMissing: true },
        { from: 'dashboards', to: 'dashboards', noErrorOnMissing: true },
        { from: '../README.md', to: '.', noErrorOnMissing: true },
      ],
    }),
  ],
  devtool: argv.mode === 'production' ? 'source-map' : 'eval-source-map',
});
