module.exports = {
  /**
   * Application configuration section
   * http://pm2.keymetrics.io/docs/usage/application-declaration/
   */
  apps: [

    // First application
    {
      name: "nina",
      max_memory_restart: "1024M",
      log_date_format: "YYYY-MM-DD HH:mm:ss SSS",
      script: "./bin/www",
      out_file: "/var/log/nina/app.log",
      error_file: "/var/log/nina/err.log",
      port: "3085",
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
      host:'123.207.79.244',
      // host: '116.62.195.14',
      ref: 'origin/master',
      repo: 'git@github.com:sqzxcv/nina.git',
      path: '/var/www/nina',
      "post-deploy": 'nvm use 8.0.0 && git pull && npm install && pm2 reload ecosystem.config.js --env production'
    },
    dev: {
      user: 'root',
      host: '116.62.195.14',
      ref: 'origin/master',
      repo: 'git@github.com:sqzxcv/videoservice.git',
      path: '/var/www/development',
      "post-deploy": 'npm install && pm2 reload ecosystem.config.js --env dev',
      env: {
        NODE_ENV: 'dev'
      }
    }
  }
};
