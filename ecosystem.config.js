module.exports = {
  /**
   * Application configuration section
   * http://pm2.keymetrics.io/docs/usage/application-declaration/
   */
  apps: [

    // First application
    {
      name: "audioInfoSpiders",
      max_memory_restart: "1024M",
      log_date_format: "YYYY-MM-DD HH:mm:ss SSS",
      script: "./app.js",
      out_file: "/var/log/audioInfoSpiders/app.log",
      error_file: "/var/log/audioInfoSpiders/err.log",
      port: "0",
      env: {
        COMMON_VARIABLE: 'true'
      },
      env_production: {
        NODE_ENV: 'production'
      }
    },
  ],

  /**
   * Deployment section
   * http://pm2.keymetrics.io/docs/usage/deployment/
   */
  deploy: {
    production: {
      user: 'root',
      // host:'123.207.79.244',
      host: '47.52.88.82',
      ref: 'origin/master',
      repo: 'git@github.com:sqzxcv/audioInfoSpiders.git',
      path: '/var/www/audioInfoSpiders',
      "post-deploy": 'nvm use 8.0.0 && git pull && npm install && pm2 reload ecosystem.config.js --env production'
    }
  }
};
